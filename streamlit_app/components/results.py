"""Result display component with foldable sections and action buttons."""

from typing import TYPE_CHECKING

import streamlit as st

if TYPE_CHECKING:
    from databao.core.executor import ExecutionResult
    from databao.core.thread import Thread


def render_response_section(text: str, has_visualization: bool) -> None:
    """Render the response text section."""
    # Response is expanded by default if no visualization
    expanded = not has_visualization

    with st.expander("📝 Response", expanded=expanded):
        st.markdown(text)


def render_code_section(code: str) -> None:
    """Render the code section (collapsed by default)."""
    with st.expander("💻 Code", expanded=False):
        # Try to detect SQL
        sql_keywords = ["SELECT", "FROM", "WHERE", "JOIN", "INSERT", "UPDATE", "DELETE"]
        if any(keyword in code.upper() for keyword in sql_keywords):
            st.code(code, language="sql")
        else:
            st.code(code, language="python")


def render_dataframe_section(result: "ExecutionResult", has_visualization: bool) -> None:
    """Render the dataframe section."""
    df = result.df
    if df is None:
        return

    # Expanded if no visualization
    expanded = not has_visualization

    with st.expander(f"📊 Data ({len(df)} rows)", expanded=expanded):
        st.dataframe(df, use_container_width=True)


def render_visualization_section(thread: "Thread") -> None:
    """Render the visualization section.

    Follows the same rendering logic as Jupyter notebooks:
    1. Vega-Lite/Altair charts: use st.vega_lite_chart for proper rendering
    2. HTML-capable objects: embed HTML
    3. PIL Images: render as images
    """
    vis_result = thread._visualization_result
    if vis_result is None:
        return

    with st.expander("📈 Visualization", expanded=True):
        plot = vis_result.plot

        if plot is None:
            st.warning("No visualization generated")
            return

        plot_type = type(plot).__name__

        # First: Try Vega-Lite chart (preferred for VegaChatResult)
        # Use st.vega_lite_chart directly with the spec and DataFrame
        if hasattr(vis_result, "spec") and hasattr(vis_result, "spec_df"):
            spec = vis_result.spec
            spec_df = vis_result.spec_df
            if spec is not None and spec_df is not None:
                try:
                    st.vega_lite_chart(spec_df, spec, use_container_width=True)
                    return
                except Exception:
                    pass  # Fall through to other methods

        # Second: Try Altair chart directly
        if "altair" in plot_type.lower() or "Chart" in plot_type:
            try:
                st.altair_chart(plot, use_container_width=True)
                return
            except Exception:
                pass  # Fall through to other methods

        # Third: Try HTML embedding for other interactive objects
        html_content = None

        # Try _repr_mimebundle_ (Vega-Embed compatible objects)
        if hasattr(plot, "_repr_mimebundle_"):
            try:
                bundle = plot._repr_mimebundle_()
                if isinstance(bundle, tuple):
                    format_dict, _metadata = bundle
                else:
                    format_dict = bundle
                if format_dict and "text/html" in format_dict:
                    html_content = format_dict["text/html"]
            except Exception:
                pass

        # Try _repr_html_
        if html_content is None and hasattr(plot, "_repr_html_"):
            try:
                html_content = plot._repr_html_()
            except Exception:
                pass

        # Try vis_result._get_plot_html()
        if html_content is None and hasattr(vis_result, "_get_plot_html"):
            try:
                html_content = vis_result._get_plot_html()
            except Exception:
                pass

        # Render HTML if we got it
        if html_content:
            st.components.v1.html(html_content, height=500, scrolling=False)
            return

        # Fourth: Check if it's a PIL Image (fallback for static images)
        if "Image" in plot_type or hasattr(plot, "_repr_png_"):
            try:
                st.image(plot)
                return
            except Exception:
                pass

        st.warning(f"Could not render visualization: {plot_type}")


def render_action_buttons(
    result: "ExecutionResult",
    thread: "Thread",
    message_index: int,
    has_visualization: bool,
    *,
    is_latest: bool = False,
) -> None:
    """Render action buttons for sections that DON'T exist yet.

    Buttons are shown only for the LATEST message and only for missing sections.
    Clicking a button will generate that section (via thread.df(), thread.code(),
    thread.plot()) and extend the message with the result. After clicking, the
    button disappears because the section now exists.

    Buttons are hidden for older messages because the thread object only maintains
    state for the most recent query.
    """
    # Only show buttons for the latest message
    if not is_latest:
        return

    # Check if we're processing a query - buttons will be disabled
    is_processing = st.session_state.get("pending_query") is not None

    buttons_to_show: list[tuple[str, str, str]] = []  # (key, label, action)

    # If no code was generated, there's nothing to show (no SQL = no data = no plot)
    # Don't show any buttons in this case
    has_code = result.code is not None
    has_data = result.df is not None

    # Show Data button only if code exists (SQL was generated) but df not yet fetched
    if has_code and not has_data:
        buttons_to_show.append(("data", "📊 Data", "generate_data"))

    # Code button: only show if there's code to display but it's not in result yet
    # (In practice, if code was generated it should already be in result.code)
    # So we don't show this button - code section appears automatically when code exists

    # Plot button: only show if we have data to plot (plot requires dataframe)
    if has_data and not has_visualization:
        buttons_to_show.append(("plot", "📈 Generate Plot", "generate_plot"))

    if not buttons_to_show:
        return

    # Render buttons in columns (disabled while processing)
    cols = st.columns(len(buttons_to_show) + 2)  # Extra columns for spacing

    for i, (_key, label, action) in enumerate(buttons_to_show):
        with cols[i]:
            button_key = f"action_{action}_{message_index}"
            clicked = st.button(label, key=button_key, use_container_width=True, disabled=is_processing)
            if clicked and not is_processing:
                handle_action_button(action, thread, message_index)


def handle_action_button(action: str, thread: "Thread", message_index: int) -> None:
    """Handle action button click by generating the missing section."""
    # Guard: don't process button clicks while a query is being processed
    if st.session_state.get("pending_query") is not None:
        return

    if action == "generate_data":
        # Generate dataframe - calls thread.df() which may trigger execution
        with st.spinner("Generating data..."):
            try:
                df = thread.df()

                # If df is still None, nothing to update (shouldn't happen with new button logic)
                if df is None:
                    return

                # Update the message in session state with a new result containing the dataframe
                # ExecutionResult is frozen, so we create a new instance
                messages = st.session_state.messages
                if message_index < len(messages) and messages[message_index].result:
                    old_result = messages[message_index].result
                    messages[message_index].result = old_result.model_copy(update={"df": df})
                # Force refresh to show the new data
                st.rerun()
            except Exception as e:
                st.error(f"Failed to generate data: {e}")

    elif action == "generate_code":
        # Generate code - calls thread.code() which may trigger execution
        with st.spinner("Generating code..."):
            try:
                code = thread.code()

                # If code is still None, nothing to update (shouldn't happen with new button logic)
                if code is None:
                    return

                # Update the message in session state with a new result containing the code
                # ExecutionResult is frozen, so we create a new instance
                messages = st.session_state.messages
                if message_index < len(messages) and messages[message_index].result:
                    old_result = messages[message_index].result
                    messages[message_index].result = old_result.model_copy(update={"code": code})
                # Force refresh to show the new code
                st.rerun()
            except Exception as e:
                st.error(f"Failed to generate code: {e}")

    elif action == "generate_plot":
        # Generate visualization - this will trigger an LLM call
        with st.spinner("Generating visualization..."):
            try:
                thread.plot()

                # Update the message in session state
                messages = st.session_state.messages
                if message_index < len(messages):
                    messages[message_index].has_visualization = True

                # Force refresh
                st.rerun()

            except Exception as e:
                st.error(f"Failed to generate visualization: {e}")


def render_execution_result(
    result: "ExecutionResult",
    thread: "Thread",
    message_index: int,
    has_visualization: bool,
    *,
    is_latest: bool = False,
) -> None:
    """Render the complete execution result with all sections."""
    # Response section (always present)
    if result.text:
        render_response_section(result.text, has_visualization)

    # Code section (if code exists)
    if result.code:
        render_code_section(result.code)

    # DataFrame section (if df exists)
    if result.df is not None:
        render_dataframe_section(result, has_visualization)

    # Visualization section (if visualization exists)
    if has_visualization or thread._visualization_result is not None:
        render_visualization_section(thread)

    # Action buttons (only for latest message)
    render_action_buttons(result, thread, message_index, has_visualization, is_latest=is_latest)
