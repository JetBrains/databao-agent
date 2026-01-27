"""Build mcp-ui resources from Databao components."""

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

from mcp_ui_server import UIResource, create_ui_resource

if TYPE_CHECKING:
    from databao.core.thread import Thread

MCP_UI_HTML_PATH = Path(__file__).parent.parent.parent / "client" / "out" / "multimodal-mcp-ui" / "index.html"


class DatabaoUIBuilder:
    """Builds mcp-ui resources from Databao threads."""

    def __init__(self) -> None:
        if not MCP_UI_HTML_PATH.exists():
            raise FileNotFoundError(f"MCP-UI HTML not found at {MCP_UI_HTML_PATH}. ")
        self._html_template = MCP_UI_HTML_PATH.read_text()

    def _create_inline_html(self, data: dict[str, Any]) -> str:
        """Create self-contained HTML with inlined data.

        Args:
            data: Data to inject into the HTML

        Returns:
            Complete HTML string with data injected
        """
        data_json = json.dumps(data)

        return self._html_template.replace(
            "window.__DATABAO_MCP_DATA__ = null;", f"window.__DATABAO_MCP_DATA__ = {data_json};"
        )

    def from_thread(self, thread: "Thread", uri: str) -> UIResource:
        """Create UI resource from a Databao thread.

        Args:
            thread: The Databao thread containing results
            uri: Unique URI for this resource

        Returns:
            UIResource that can be returned from MCP tools
        """
        from edaplot.data_utils import spec_add_data

        from databao.visualizers.vega_chat import VegaChatResult

        data = {}

        text = thread.text()
        if text:
            data["text"] = text

        df = thread.df()
        if df is not None:
            data["dataframeHtmlContent"] = self._dataframe_to_html(df)

        try:
            plot = thread.plot()
            if isinstance(plot, VegaChatResult) and plot.spec and plot.spec_df is not None:
                spec_with_data = spec_add_data(plot.spec.copy(), plot.spec_df)
                data["spec"] = spec_with_data
        except Exception:
            pass

        html_content = self._create_inline_html(data)

        return create_ui_resource(
            {"uri": uri, "content": {"type": "rawHtml", "htmlString": html_content}, "encoding": "text"}
        )

    def _dataframe_to_html(self, df: "Any") -> str:
        """Convert DataFrame to HTML"""
        import pandas as pd

        if len(df) > 20:
            first_10 = df.head(10)
            last_10 = df.tail(10)
            separator_data = {col: "..." for col in df.columns}
            separator_df = pd.DataFrame([separator_data], index=["..."])
            truncated_df = pd.concat([first_10, separator_df, last_10])
            return truncated_df.to_html()
        else:
            return df.to_html()
