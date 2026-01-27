export interface DatabaoMCPData {
  text?: string;
  dataframeHtmlContent?: string;
  spec?: object;
}

declare global {
  interface Window {
    __DATABAO_MCP_DATA__: DatabaoMCPData | null;
  }
}
