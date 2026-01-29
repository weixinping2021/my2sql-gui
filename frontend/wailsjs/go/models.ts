export namespace main {
	
	export class AnalyzeRequest {
	    connectionString: string;
	    outputDir: string;
	    databases: string[];
	    tables: string[];
	    threads: number;
	    includeDDL: boolean;
	    includeInsert: boolean;
	    includeUpdate: boolean;
	    includeDelete: boolean;
	    worktype: string;
	    startDatetime: string;
	    stopDatetime: string;
	
	    static createFrom(source: any = {}) {
	        return new AnalyzeRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.connectionString = source["connectionString"];
	        this.outputDir = source["outputDir"];
	        this.databases = source["databases"];
	        this.tables = source["tables"];
	        this.threads = source["threads"];
	        this.includeDDL = source["includeDDL"];
	        this.includeInsert = source["includeInsert"];
	        this.includeUpdate = source["includeUpdate"];
	        this.includeDelete = source["includeDelete"];
	        this.worktype = source["worktype"];
	        this.startDatetime = source["startDatetime"];
	        this.stopDatetime = source["stopDatetime"];
	    }
	}
	export class BinlogResult {
	    id: number;
	    operation: string;
	    database: string;
	    table: string;
	    records: number;
	    timestamp: string;
	
	    static createFrom(source: any = {}) {
	        return new BinlogResult(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.id = source["id"];
	        this.operation = source["operation"];
	        this.database = source["database"];
	        this.table = source["table"];
	        this.records = source["records"];
	        this.timestamp = source["timestamp"];
	    }
	}

}

