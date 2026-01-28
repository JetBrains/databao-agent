"""MCP server for Databao with MCP Apps support."""

import os
import sys
from pathlib import Path

import uvicorn
from mcp import types
from mcp.server.fastmcp import FastMCP
from starlette.middleware.cors import CORSMiddleware

VIEW_URI = "ui://databao/visualizer.html"
HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "3001"))

HTML_PATH = Path(__file__).parent.parent.parent / "client" / "out" / "multimodal-mcp-ui" / "index.html"

mcp = FastMCP("Databao Visualizer", stateless_http=True)


@mcp.tool(meta={"ui": {"resourceUri": VIEW_URI, "ui/resourceUri": VIEW_URI}})
def visualize_data(query: str, data: list[dict]) -> list[types.TextContent]:
    """Create interactive data visualizations with charts and tables.

    CRITICAL REQUIREMENTS:
    1. MUST provide 'query' parameter - describes what type of chart to create
    2. MUST provide 'data' parameter - array of data objects (cannot be empty)

    This tool does NOT access databases or files. You MUST provide data directly.

    Common patterns:
    - User provides data → extract and pass it in 'data' parameter
    - User wants calculation visualized → calculate first, then visualize results
    - User asks without data → generate sample data or ask user for it

    Args:
        query: Natural language description of the visualization.
               Be specific about:
               - Chart type: bar, line, scatter, pie, area, etc.
               - X-axis field name
               - Y-axis field name
               - Any grouping or aggregation

               Good examples:
               ✓ 'Create a bar chart with name on x-axis and value on y-axis'
               ✓ 'Line chart showing sales over time (month on x, sales on y)'
               ✓ 'Scatter plot: age vs salary'

               Avoid vague queries:
               ✗ 'Show a chart'
               ✗ 'Visualize the data'

        data: REQUIRED - Array of data objects (dictionaries).

              Format: Each object is one data point with named fields.
              [{field1: value1, field2: value2, ...}, {field1: value1, field2: value2, ...}]

              Examples:
              ✓ [{'category': 'A', 'amount': 10}, {'category': 'B', 'amount': 20}]
              ✓ [{'date': '2024-01', 'revenue': 1000, 'costs': 800}]

              Cannot be empty or null - must contain at least one object.

    Returns:
        Text content with visualization data encoded as JSON
    """
    import concurrent.futures
    import json
    import sys
    import traceback

    try:
        # Suppress stdout during execution to avoid polluting MCP stdio protocol
        def execute_with_suppressed_output():
            import pandas as pd

            from databao import new_agent

            df = pd.DataFrame(data)
            temp_agent = new_agent()

            # Generate context from first few rows
            context_rows = df.head(5).to_dict(orient="records")
            context = f"Sample data: {context_rows}"

            temp_agent.add_df(df, name="data", context=context)
            thread = temp_agent.thread()

            # Redirect stdout to stderr so MCP protocol isn't polluted
            old_stdout = sys.stdout
            sys.stdout = sys.stderr
            try:
                thread.ask(query)
            finally:
                sys.stdout = old_stdout

            return thread

        # Run in thread pool to avoid blocking/conflicting with existing event loop
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(execute_with_suppressed_output)
            thread = future.result()

        from edaplot.data_utils import spec_add_data

        from databao.visualizers.vega_chat import VegaChatResult

        viz_data = {}

        text = thread.text()
        if text:
            viz_data["text"] = text

        df = thread.df()
        if df is not None:
            viz_data["dataframeHtmlContent"] = _dataframe_to_html(df)

        try:
            plot = thread.plot()
            if isinstance(plot, VegaChatResult) and plot.spec and plot.spec_df is not None:
                spec_with_data = spec_add_data(plot.spec.copy(), plot.spec_df)
                viz_data["spec"] = spec_with_data
        except Exception:
            pass

        json_data = json.dumps(viz_data)
        return [types.TextContent(type="text", text=json_data)]

    except Exception as e:
        error_msg = str(e)
        stack_trace = traceback.format_exc()
        error_data = {"error": error_msg, "text": f"Error: {error_msg}", "traceback": stack_trace}
        return [types.TextContent(type="text", text=json.dumps(error_data))]


def _dataframe_to_html(df) -> str:
    """Convert DataFrame to HTML (same logic as in jupiter_widget.py)."""
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


@mcp.resource(
    VIEW_URI,
    mime_type="text/html;profile=mcp-app",
)
def view() -> str:
    """View HTML resource for the Databao visualizer."""
    if not HTML_PATH.exists():
        raise FileNotFoundError(f"MCP-UI HTML not found at {HTML_PATH}")
    return HTML_PATH.read_text()


def main() -> None:
    """Main entry point for the MCP server."""
    if "--stdio" in sys.argv or len(sys.argv) == 1:
        mcp.run(transport="stdio")
    else:
        # HTTP mode for testing with basic-host - with CORS
        app = mcp.streamable_http_app()
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["*"],
            allow_headers=["*"],
        )
        print(f"Databao MCP Server listening on http://{HOST}:{PORT}/mcp")
        uvicorn.run(app, host=HOST, port=PORT)


if __name__ == "__main__":
    main()
