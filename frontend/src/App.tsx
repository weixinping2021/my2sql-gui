import React, { useState, useEffect, useRef } from 'react';
import { 
  Layout, Card, Form, Input, Button, Select, Switch, 
  DatePicker, InputNumber, Space, Table, Tag, Typography, 
  message, ConfigProvider, Row, Col, Modal, Divider, Drawer, Radio, Badge
} from 'antd';
import { 
  DatabaseOutlined, FolderOpenOutlined, PlayCircleOutlined, 
  ExclamationCircleOutlined, CheckOutlined, FileSearchOutlined,
  SettingOutlined, FilterOutlined, ConsoleSqlOutlined,
  DeleteOutlined, StopOutlined, SyncOutlined, HistoryOutlined, ArrowRightOutlined
} from '@ant-design/icons';
import zhCN from 'antd/locale/zh_CN';

// 导入 Wails 运行时和生成的 Go 函数
// @ts-ignore
import { TestConnection, GetTables, AnalyzeBinlog, SelectFolder, ParseBinlogStatus, StopAnalyze } from '../wailsjs/go/main/App';
// @ts-ignore
import { EventsOn, EventsOff } from '../wailsjs/runtime/runtime';

const { Header, Content, Footer } = Layout;
const { Title, Text } = Typography;
const { RangePicker } = DatePicker;

interface ResultRow {
  id: number;
  operation: string;
  database: string;
  table: string;
  records: number;
  timestamp: string;
}

const App: React.FC = () => {
  const [form] = Form.useForm();
  
  // --- 状态控制 ---
  const [loading, setLoading] = useState(false);
  const [connLoading, setConnLoading] = useState(false);
  const [connStatus, setConnStatus] = useState<'none' | 'success' | 'error'>('none');
  const [availableDbs, setAvailableDbs] = useState<string[]>([]);
  const [availableTables, setAvailableTables] = useState<string[]>([]);
  const [results, setResults] = useState<ResultRow[]>([]);
  const [isModalVisible, setIsModalVisible] = useState(false);

  // --- 日志相关状态 ---
  const [logVisible, setLogVisible] = useState(false);
  const [logs, setLogs] = useState<string[]>([]);
  const logEndRef = useRef<HTMLDivElement>(null);

  // --- 表单联动监听 ---
  const outputDirValue = Form.useWatch('outputDir', form);
  const includeDDL = Form.useWatch('includeDDL', form);
  const includeInsert = Form.useWatch('includeInsert', form);
  const includeUpdate = Form.useWatch('includeUpdate', form);
  const includeDelete = Form.useWatch('includeDelete', form);
  const sqlType = Form.useWatch('sqlType', form);
  const isDMLActive = includeInsert || includeUpdate || includeDelete;

  // 1. 监听后端日志事件
  useEffect(() => {
    const handler = (msg: string) => {
      setLogs(prev => [...prev.slice(-999), msg]); 
    };
    EventsOn("backend-log", handler);
    return () => EventsOff("backend-log");
  }, []);

  // 2. 日志自动滚动到底部
  useEffect(() => {
    if (logVisible && logEndRef.current) {
      logEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [logs, logVisible]);

  // 3. 停止任务函数（核心修改）
  const handleStopTask = async () => {
    try {
      await StopAnalyze();
      setLoading(false); // 关键：立即恢复按钮状态
      message.warning('解析任务已强制停止');
    } catch (e) {
      message.error('停止指令发送失败');
    }
  };

  // 4. 处理 DDL 切换冲突逻辑
  const handleDDLChange = (checked: boolean) => {
    if (checked && (isDMLActive || sqlType === 'rollback')) {
      Modal.confirm({
        title: '解析模式调整',
        icon: <ExclamationCircleOutlined style={{ color: '#faad14' }} />,
        content: sqlType === 'rollback' 
          ? '回滚模式不支持 DDL 语句。开启 DDL 将自动切换为正向模式并清空 DML 选项，是否继续？' 
          : '开启 DDL 模式将自动禁用 DML 操作（Insert/Update/Delete）。是否继续？',
        okText: '确认切换',
        onOk() { 
          form.setFieldsValue({ 
            includeInsert: false, includeUpdate: false, includeDelete: false, 
            includeDDL: true, sqlType: 'forward' 
          }); 
        },
      });
    } else {
      form.setFieldsValue({ includeDDL: checked });
    }
  };

  // 5. 测试连接
  const onTestConnection = async () => {
    try {
      const values = await form.validateFields(['connectionString']);
      setConnLoading(true);
      form.setFieldsValue({ databases: undefined, tables: [] });
      const dbs = await TestConnection(values.connectionString);
      setAvailableDbs(dbs);
      setConnStatus('success');
      message.success('数据库连接成功');
    } catch (err: any) {
      setConnStatus('error');
      message.error(`连接失败: ${err}`);
    } finally {
      setConnLoading(false);
    }
  };

  // 6. 查看结果报告
  const handleViewSummary = async () => {
    if (!outputDirValue) return message.warning('请先设置保存路径');
    try {
      const res = await ParseBinlogStatus(`${outputDirValue}/binlog_status.txt`);
      setResults(res || []);
      if (res && res.length > 0) setIsModalVisible(true);
      else message.info('未发现有效的分析结果');
    } catch (err) {
      message.error('读取报告文件失败');
    }
  };

  // 7. 提交任务
  const onHandleSubmit = async (values: any) => {
    setLogs([]);
    setLogVisible(true);
    setLoading(true);
    try {
      const payload = { 
        ...values, 
        workType: values.sqlType, 
        databases: values.databases ? [values.databases] : [],
        startDatetime: values.timeRange?.[0] ? values.timeRange[0].format('YYYY-MM-DD HH:mm:ss') : '',
        stopDatetime: values.timeRange?.[1] ? values.timeRange[1].format('YYYY-MM-DD HH:mm:ss') : '',
      };
      await AnalyzeBinlog(payload);
      message.success('解析任务执行完毕');
    } catch (e: any) { 
      message.error(`执行出错: ${e}`); 
    } finally { 
      setLoading(false); 
    }
  };

  return (
    <ConfigProvider locale={zhCN} theme={{ 
        token: { borderRadius: 8, colorPrimary: '#1677ff' },
        components: { Card: { headerBg: '#fafafa' } }
      }}>
      <Layout style={{ minHeight: '100vh', background: '#f5f7f9' }}>
        
        {/* --- 顶部下拉日志面板 --- */}
        <Drawer
          title={
            <div style={{ display: 'flex', justifyContent: 'space-between', width: '100%', alignItems: 'center', paddingRight: 24 }}>
              <Space>
                <ConsoleSqlOutlined /> 
                实时执行监控 
                {loading && <Badge status="processing" text={<Text type="secondary" style={{fontSize: 12}}>正在解析...</Text>} />}
              </Space>
              <Space>
                {/* 停止按钮移至此处 */}
                {loading && (
                  <Button size="small" danger type="primary" icon={<StopOutlined />} onClick={handleStopTask}>
                    停止分析
                  </Button>
                )}
                <Button size="small" icon={<DeleteOutlined />} onClick={() => setLogs([])}>清空日志</Button>
                <Button size="small" onClick={() => setLogVisible(false)}>收起</Button>
              </Space>
            </div>
          }
          placement="top"
          onClose={() => setLogVisible(false)}
          open={logVisible}
          height={450}
          styles={{ body: { background: '#1e1e1e', padding: '12px' } }}
          closable={false}
        >
          <div style={{ fontFamily: 'monospace', fontSize: '12px', color: '#d4d4d4', whiteSpace: 'pre-wrap' }}>
            {logs.map((log, i) => (
              <div key={i} style={{ borderBottom: '1px solid #2d2d2d', padding: '2px 0' }}>
                <span style={{ color: '#6a9955', marginRight: 8 }}>[{new Date().toLocaleTimeString()}]</span>
                {log}
              </div>
            ))}
            <div ref={logEndRef} />
          </div>
        </Drawer>

        <Header style={{ background: '#fff', padding: '0 40px', display: 'flex', alignItems: 'center', borderBottom: '1px solid #e8e8e8', height: 64, zIndex: 100 }}>
          <Space style={{ flex: 1 }}>
            <div style={{ background: '#1677ff', width: 32, height: 32, borderRadius: 6, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              <DatabaseOutlined style={{ color: '#fff' }} />
            </div>
            <Title level={4} style={{ margin: 0 }}>My2SqlGUI</Title>
          </Space>
          <Button icon={<ConsoleSqlOutlined />} onClick={() => setLogVisible(true)} danger={loading}>
            {loading ? "解析进行中..." : "查看运行日志"}
            {logs.length > 0 && <Badge dot status="processing" style={{ marginLeft: 8 }} />}
          </Button>
        </Header>

        <Content style={{ padding: '24px 40px' }}>
          <div style={{ maxWidth: 1100, margin: '0 auto' }}>
            
            <Form
              form={form}
              layout="vertical"
              initialValues={{ 
                sqlType: 'forward', connectionString: 'root:password@tcp(127.0.0.1:3306)', 
                threads: 4, includeInsert: true, includeUpdate: true, includeDelete: true 
              }}
              onFinish={onHandleSubmit}
            >
              <Card size="small" title={<Space><SettingOutlined />连接与输出</Space>} style={{ marginBottom: 20 }}>
                <Row gutter={24}>
                  <Col span={12}>
                    <Form.Item label="MySQL 连接字符串" name="connectionString" rules={[{ required: true }]}>
                      <Space.Compact style={{ width: '100%' }}>
                        <Input placeholder="root:pass@tcp(127.0.0.1:3306)" variant="filled" spellCheck={false} />
                        <Button type="primary" onClick={onTestConnection} loading={connLoading}>测试</Button>
                      </Space.Compact>
                    </Form.Item>
                  </Col>
                  <Col span={12}>
                    <Form.Item label="保存目录" name="outputDir" rules={[{ required: true }]}>
                      <Space.Compact style={{ width: '100%' }}>
                        <Input value={outputDirValue} readOnly placeholder="请选择输出文件夹" variant="filled" />
                        <Button icon={<FolderOpenOutlined />} onClick={async () => {
                          const f = await SelectFolder();
                          if(f) form.setFieldsValue({ outputDir: f });
                        }} />
                      </Space.Compact>
                    </Form.Item>
                  </Col>
                </Row>
              </Card>

              <Row gutter={20}>
                <Col span={15}>
                  <Card size="small" title={<Space><FilterOutlined />范围限制</Space>} style={{ height: '100%' }}>
                    <Form.Item label="目标数据库" name="databases" rules={[{ required: true }]}>
                      <Select 
                        showSearch 
                        spellCheck={false}
                        options={availableDbs.map(d => ({ label: d, value: d }))} 
                        onChange={async (db) => {
                          const tbs = await GetTables(form.getFieldValue('connectionString'), [db]);
                          setAvailableTables(tbs);
                        }}
                      />
                    </Form.Item>
                    <Form.Item label="目标表名" name="tables">
                      <Select mode="multiple" spellCheck={false} placeholder="不选则分析全库" options={availableTables.map(t => ({ label: t, value: t }))} />
                    </Form.Item>
                    <Form.Item label="时间段过滤" name="timeRange">
                      <RangePicker showTime style={{ width: '100%' }} />
                    </Form.Item>
                  </Card>
                </Col>

                <Col span={9}>
                  <Card size="small" title="控制面板" style={{ height: '100%' }}>
                    <div style={{ marginBottom: 20, padding: 12, background: '#f8fafc', borderRadius: 8, border: '1px solid #e2e8f0' }}>
                      <Text strong style={{ display: 'block', marginBottom: 10 }}>生成 SQL 类型</Text>
                      <Form.Item name="sqlType" noStyle>
                        <Radio.Group buttonStyle="solid" style={{ width: '100%' }}>
                          <Radio.Button value="forward" style={{ width: '50%', textAlign: 'center' }}>
                            <ArrowRightOutlined /> 正向
                          </Radio.Button>
                          <Radio.Button value="rollback" style={{ width: '50%', textAlign: 'center' }}>
                            <HistoryOutlined /> 回滚
                          </Radio.Button>
                        </Radio.Group>
                      </Form.Item>
                    </div>

                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 16 }}>
                      <Text>并发线程</Text>
                      <Form.Item name="threads" noStyle><InputNumber min={1} max={64} /></Form.Item>
                    </div>

                    <div style={{ background: '#fff', border: '1px solid #f0f0f0', borderRadius: 8, padding: '10px 12px', marginBottom: 20 }}>
                      <Space direction="vertical" style={{ width: '100%' }}>
                         <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                            <Text>DML 操作 (I/U/D)</Text>
                            <Space>
                              <Form.Item name="includeInsert" valuePropName="checked" noStyle><Switch size="small" disabled={includeDDL} /></Form.Item>
                              <Form.Item name="includeUpdate" valuePropName="checked" noStyle><Switch size="small" disabled={includeDDL} /></Form.Item>
                              <Form.Item name="includeDelete" valuePropName="checked" noStyle><Switch size="small" disabled={includeDDL} /></Form.Item>
                            </Space>
                         </div>
                         <Divider style={{ margin: '8px 0' }} />
                         <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                            <Text disabled={sqlType === 'rollback'}>结构变更 (DDL)</Text>
                            <Form.Item name="includeDDL" valuePropName="checked" noStyle>
                              <Switch size="small" checked={includeDDL} onChange={handleDDLChange} disabled={sqlType === 'rollback'} />
                            </Form.Item>
                         </div>
                      </Space>
                    </div>

                    <Space direction="vertical" style={{ width: '100%' }}>
                      <Button 
                        type="primary" block size="large" htmlType="submit" 
                        loading={loading} disabled={connStatus !== 'success'} 
                        icon={<PlayCircleOutlined />} 
                        style={{ height: 48 }}
                      >
                        {loading ? '后台解析中...' : '开始分析'}
                      </Button>

                      <Button 
                        block 
                        icon={<FileSearchOutlined />} 
                        onClick={handleViewSummary} 
                        style={{ marginTop: 8, height: 40, color: '#1677ff', borderColor: '#1677ff' }}
                      >
                        查看结果报告
                      </Button>
                    </Space>
                  </Card>
                </Col>
              </Row>
            </Form>
          </div>
        </Content>

        <Modal 
          title={<Space><FileSearchOutlined style={{ color: '#1677ff' }} /> 数据变动摘要</Space>} 
          open={isModalVisible} 
          onCancel={() => setIsModalVisible(false)} 
          width={900} 
          centered
          footer={[<Button key="ok" type="primary" onClick={() => setIsModalVisible(false)}>确 认</Button>]}
        >
          <Table 
            dataSource={results} 
            rowKey="id" 
            size="small"
            columns={[
              { title: '类型', dataIndex: 'operation', key: 'operation', render: (t) => <Tag color={t==='INSERT'?'green':t==='DELETE'?'red':'orange'}>{t}</Tag> },
              { title: '数据库', dataIndex: 'database', key: 'database' },
              { title: '数据表', dataIndex: 'table', key: 'table' },
              { title: '变动行数', dataIndex: 'records', align: 'right' },
              { title: '时间点', dataIndex: 'timestamp' },
            ]} 
          />
        </Modal>

        <Footer style={{ textAlign: 'center', color: '#bfbfbf', fontSize: 12 }}>
          My2Sql Pro Visual Console ©2026 Powered by Wails & Ant Design
        </Footer>
      </Layout>
    </ConfigProvider>
  );
};

export default App;