import {
  DataframeTable,
  TabModel,
  VegaChart,
  Tabs,
} from "@databao/multimodal-tabs";
import { Text, Theme } from "@radix-ui/themes";

import styles from "./App.module.css";

function App() {
  const data = window.__DATABAO_MCP_DATA__ || null;

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
        <div className={styles.loader}>
          <Text color="gray">No data available</Text>
        </div>
      </Theme>
    );
  }

  const tabs: TabModel[] = [];

  if (data?.dataframeHtmlContent) {
    tabs.push({
      type: "DATAFRAME",
      title: "Data",
      content: () => renderTable(data.dataframeHtmlContent),
    });
  }

  if (data?.spec) {
    tabs.push({
      type: "CHART",
      title: "Chart",
      content: () => renderChart(data.spec ?? null),
    });
  }

  if (data?.text) {
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

export default App;
