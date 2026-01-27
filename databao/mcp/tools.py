"""MCP tool implementations."""

from typing import Any

from mcp.types import TextContent, Tool

from databao.mcp.ui_builder import DatabaoUIBuilder


class DatabaoMCPTools:
    """Handles MCP tool calls for Databao."""

    def __init__(self) -> None:
        self.ui_builder = DatabaoUIBuilder()
        self._query_counter = 0

    async def handle_visualize_data(self, arguments: dict[str, Any]) -> list[Any]:
        """Handle visualize_data tool call.

        Args:
            arguments: {
                "query": str - What to visualize
                "data": list[dict] - Data to visualize
            }

        Returns:
            List containing text response and UI resource
        """
        import sys
        import traceback

        try:
            query = arguments["query"]
            data = arguments["data"]

            self._query_counter += 1
            uri = f"ui://databao/query/{self._query_counter}"

            thread = await self._create_thread_from_data(data, query)

            ui_resource = self.ui_builder.from_thread(thread, uri)

            return [TextContent(type="text", text=f"Visualized: '{query}' with {len(data)} data points"), ui_resource]

        except Exception as e:
            error_msg = f"Error in visualize_data: {str(e)}"
            print(error_msg, file=sys.stderr)
            print(traceback.format_exc(), file=sys.stderr)
            return [TextContent(type="text", text=f"Error: {str(e)}")]

    async def _create_thread_from_data(self, data: list[dict[str, Any]], query: str) -> "Any":
        """Create a thread with temporary agent for provided data.

        Args:
            data: List of data objects
            query: Visualization query

        Returns:
            Thread with data registered and query executed
        """
        import asyncio
        import sys

        import pandas as pd

        from databao import new_agent

        df = pd.DataFrame(data)
        agent = new_agent()

        # columns_info = ", ".join([f"{col} ({df[col].dtype})" for col in df.columns])
        # context = (
        #     f"User-provided dataset with {len(df)} rows and {len(df.columns)} columns. "
        #     f"Columns: {columns_info}. "
        #     f"Sample data: {df.head(3).to_dict('records')}. "
        #     f"Use this data directly to create the requested visualization."
        # )

        agent.add_df(df, name="data")

        thread = agent.thread()

        # Suppress stdout during execution to avoid polluting MCP stdio protocol
        def execute_with_suppressed_output():
            # Redirect stdout to stderr so MCP protocol isn't polluted
            old_stdout = sys.stdout
            sys.stdout = sys.stderr
            try:
                thread.ask(query)
            finally:
                sys.stdout = old_stdout

        # Run the synchronous ask() in a thread pool to avoid blocking the event loop
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, execute_with_suppressed_output)

        return thread


TOOLS = [
    Tool(
        name="visualize_data",
        description=(
            "Create interactive data visualizations with charts and tables.\n\n"
            "CRITICAL REQUIREMENTS:\n"
            "1. MUST provide 'query' parameter - describes what type of chart to create\n"
            "2. MUST provide 'data' parameter - array of data objects (cannot be empty)\n\n"
            "This tool does NOT access databases or files. You MUST provide data directly.\n\n"
            "Common patterns:\n"
            "- User provides data → extract and pass it in 'data' parameter\n"
            "- User wants calculation visualized → calculate first, then visualize results\n"
            "- User asks without data → generate sample data or ask user for it"
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": (
                        "Natural language description of the visualization.\n\n"
                        "Be specific about:\n"
                        "- Chart type: bar, line, scatter, pie, area, etc.\n"
                        "- X-axis field name\n"
                        "- Y-axis field name\n"
                        "- Any grouping or aggregation\n\n"
                        "Good examples:\n"
                        "✓ 'Create a bar chart with name on x-axis and value on y-axis'\n"
                        "✓ 'Line chart showing sales over time (month on x, sales on y)'\n"
                        "✓ 'Scatter plot: age vs salary'\n\n"
                        "Avoid vague queries:\n"
                        "✗ 'Show a chart'\n"
                        "✗ 'Visualize the data'"
                    ),
                },
                "data": {
                    "type": "array",
                    "description": (
                        "REQUIRED - Array of data objects (dictionaries).\n\n"
                        "Format: Each object is one data point with named fields.\n"
                        "[{field1: value1, field2: value2, ...}, {field1: value1, field2: value2, ...}]\n\n"
                        "Examples:\n"
                        "✓ [{'category': 'A', 'amount': 10}, {'category': 'B', 'amount': 20}]\n"
                        "✓ [{'date': '2024-01', 'revenue': 1000, 'costs': 800}, {'date': '2024-02', 'revenue': 1200, 'costs': 900}]\n"
                        "✓ [{'name': 'Alice', 'age': 25, 'salary': 50000}]\n\n"
                        "Field names should match what you reference in the query parameter.\n\n"
                        "Cannot be empty or null - must contain at least one object."
                    ),
                    "items": {"type": "object"},
                    "minItems": 1,
                },
            },
            "required": ["query", "data"],
        },
    )
]
