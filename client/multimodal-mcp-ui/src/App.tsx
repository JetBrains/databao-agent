import {
  DataframeTable,
  TabModel,
  Tabs,
  VegaChart,
} from "@databao/multimodal-tabs";
import { App } from "@modelcontextprotocol/ext-apps";
import { Text, Theme } from "@radix-ui/themes";
import { useEffect, useState } from "react";

import styles from "./App.module.css";

interface DatabaoMCPData {
  text?: string;
  dataframeHtmlContent?: string;
  spec?: object;
}

interface MCPContent {
  type: string;
  text?: string;
  [key: string]: unknown;
}

interface MCPToolResult {
  content?: MCPContent[];
  [key: string]: unknown;
}

function DatabaoApp() {
  const [data, setData] = useState<DatabaoMCPData | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const initApp = async () => {
      try {
        const app = new App({
          name: "Databao Visualizer",
          version: "1.0.0",
        });

        app.ontoolresult = (result: MCPToolResult) => {
          const content = result.content?.find((c) => c.type === "text");
          if (content?.text) {
            try {
              const vizData = JSON.parse(content.text) as DatabaoMCPData;
              setData(vizData);
            } catch (err) {
              setError("Failed to parse visualization data: " + err);
            }
          }
        };

        await app.connect();
      } catch (err) {
        setError("Failed to initialize MCP App: " + String(err));
      }
    };

    initApp();
  }, []);

  const renderChart = (spec: object | null) => {
    if (!spec) {
      return <Text color="gray">No chart available</Text>;
    }
    return <VegaChart spec={spec} />;
  };

  const renderDescription = (text?: string) => {
    if (!text) {
      return <Text color="gray">No description available</Text>;
    }
    return <Text color="gray">{text}</Text>;
  };

  const renderTable = (htmlContent?: string) => {
    if (!htmlContent) {
      return <Text color="gray">No data available</Text>;
    }
    return <DataframeTable htmlContent={htmlContent} />;
  };

  if (!data) {
    return (
      <Theme>
        <div className={styles.appContainer}>
          <div style={{ padding: "40px", textAlign: "center" }}>
            {error ? (
              <Text color="red" size="3">
                {error}
              </Text>
            ) : (
              <Text color="gray" size="3">
                Waiting for data...
              </Text>
            )}
          </div>
        </div>
      </Theme>
    );
  }

  const tabs: TabModel[] = [];

  if (data.dataframeHtmlContent) {
    tabs.push({
      type: "DATAFRAME",
      title: "Data",
      content: () => renderTable(data.dataframeHtmlContent),
    });
  }

  if (data.spec) {
    tabs.push({
      type: "CHART",
      title: "Chart",
      content: () => renderChart(data.spec ?? null),
    });
  }

  if (data.text) {
    tabs.push({
      type: "DESCRIPTION",
      title: "Description",
      content: () => renderDescription(data.text),
    });
  }

  return (
    <Theme>
      <div className={styles.appContainer}>
        <Tabs tabs={tabs} />
      </div>
    </Theme>
  );
}

export default DatabaoApp;
