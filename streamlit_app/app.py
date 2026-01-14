"""Databao Streamlit Web Interface - Main Application."""

import logging
from pathlib import Path
from typing import cast

import streamlit as st

import databao
import databao.dce
from databao.core.agent import Agent
from databao.core.thread import Thread
from databao.dce import (
    DCEProject,
    DCEProjectStatus,
    create_all_connections,
    find_best_project,
    get_all_context,
)
from streamlit_app.components.chat import render_chat_interface
from streamlit_app.components.sidebar import render_sidebar

logger = logging.getLogger(__name__)

# Page config - use Databao logo as favicon
from pathlib import Path

_ASSETS_DIR = Path(__file__).parent / "assets"
_FAVICON = _ASSETS_DIR / "bao.png"

st.set_page_config(
    page_title="Databao",
    page_icon=str(_FAVICON) if _FAVICON.exists() else "🎋",
    layout="wide",
    initial_sidebar_state="expanded",
)


def init_session_state() -> None:
    """Initialize session state variables."""
    if "dce_project" not in st.session_state:
        st.session_state.dce_project = None
    if "agent" not in st.session_state:
        st.session_state.agent = None
    if "thread" not in st.session_state:
        st.session_state.thread = None
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "app_status" not in st.session_state:
        st.session_state.app_status = "initializing"
    if "error_message" not in st.session_state:
        st.session_state.error_message = None
    if "executor_type" not in st.session_state:
        st.session_state.executor_type = "lighthouse"


def get_current_project() -> DCEProject | None:
    """Get the current DCE project, auto-detecting if needed."""
    # If we already have a project, return it
    if st.session_state.dce_project is not None:
        return cast(DCEProject, st.session_state.dce_project)

    # Use find_best_project which handles detection and selection
    project = find_best_project()
    if project is not None:
        st.session_state.dce_project = project

    return project


def initialize_agent(project: DCEProject) -> Agent | None:
    """Initialize Databao agent with DCE project data."""
    if st.session_state.agent is not None:
        return cast(Agent, st.session_state.agent)

    if project.status == DCEProjectStatus.NO_BUILD:
        st.session_state.error_message = (
            "DCE project found but no build output. Run 'nemory build' first."
        )
        return None

    try:
        # Create streaming writer for capturing thinking output
        from streamlit_app.streaming import StreamingWriter

        writer = StreamingWriter()
        st.session_state.streaming_writer = writer

        # Get executor type from session state
        executor_type = st.session_state.get("executor_type", "lighthouse")

        # Create agent with writer and executor type
        agent = databao.new_agent(
            writer=writer,
            executor_type=executor_type,
        )

        # Get connections from DCE configs
        connections = create_all_connections(project.path)

        if not connections:
            st.session_state.error_message = "No datasource connections found in DCE project."
            return None

        # Get context from DCE outputs
        run_dir = project.latest_run_dir
        db_contexts: list[databao.dce.DatabaseContext] = []
        file_contexts: list[databao.dce.FileContext] = []
        if run_dir:
            db_contexts, file_contexts = get_all_context(run_dir)

        # Build a map of database_id -> context
        db_context_map = {ctx.database_id: ctx.context_text for ctx in db_contexts}

        # Add connections to agent
        for conn_info in connections:
            # Try to find matching context
            context = db_context_map.get(conn_info.name) or db_context_map.get(conn_info.db_type)
            agent.add_db(conn_info.connection, name=conn_info.name, context=context)

        # Add file contexts as general context
        for file_ctx in file_contexts:
            agent.add_context(file_ctx.context_text)

        st.session_state.agent = agent
        st.session_state.app_status = "ready"
        return agent

    except Exception as e:
        logger.exception("Failed to initialize agent")
        st.session_state.error_message = f"Failed to initialize agent: {e}"
        st.session_state.app_status = "error"
        return None


def get_or_create_thread() -> Thread | None:
    """Get or create a thread for the current agent."""
    if st.session_state.thread is not None:
        return cast(Thread, st.session_state.thread)

    agent: Agent | None = st.session_state.agent
    if agent is None:
        return None

    thread = agent.thread(stream_ask=True, stream_plot=False)
    st.session_state.thread = thread
    return thread


def render_empty_state() -> None:
    """Render the empty state when no DCE project is found."""
    st.title("Databao")
    st.markdown("---")

    st.warning("No DCE project detected.")

    st.markdown("""
    ### Getting Started

    To use Databao, you need a DCE (Databao Context Engine) project with configured datasources.

    **Set up a new project:**
    ```bash
    nemory init
    nemory datasource add
    nemory build
    ```

    Then reload this page.

    **Or select an existing project path below:**
    """)

    # Manual path selector
    custom_path = st.text_input(
        "Project path",
        placeholder="/path/to/your/nemory-project",
        help="Enter the path to a directory containing nemory.ini",
    )

    if custom_path:
        path = Path(custom_path).expanduser().resolve()
        if path.is_dir():
            from databao.dce.project import validate_project

            project = validate_project(path)
            if project.status != DCEProjectStatus.NOT_FOUND:
                st.session_state.dce_project = project
                st.session_state.dce_candidates = [project]
                st.rerun()
            else:
                st.error(f"No DCE project found at {path}")
        else:
            st.error(f"Path does not exist: {path}")


def render_error_state() -> None:
    """Render error state."""
    st.title("Databao")
    st.markdown("---")

    st.error(st.session_state.error_message or "An error occurred")

    if st.button("🔄 Retry"):
        # Reset state
        st.session_state.agent = None
        st.session_state.thread = None
        st.session_state.app_status = "initializing"
        st.session_state.error_message = None
        st.rerun()


def render_no_build_state(project: DCEProject) -> None:
    """Render state when DCE project has no build output."""
    st.title("Databao")
    st.markdown("---")

    st.warning(f"DCE project found at `{project.path}` but no build output exists.")

    st.markdown("""
    ### Build Required

    The DCE project needs to be built before Databao can use it.

    Run the following command:
    ```bash
    nemory build
    ```

    Then reload this page.
    """)

    if st.button("🔄 Check Again"):
        st.session_state.dce_project = None
        st.session_state.dce_candidates = []
        st.rerun()


def main() -> None:
    """Main application entry point."""
    init_session_state()

    # Detect project
    project = get_current_project()

    # Handle different states BEFORE initializing agent
    if project is None:
        render_sidebar(project)
        render_empty_state()
        return

    if project.status == DCEProjectStatus.NO_BUILD:
        render_sidebar(project)
        render_no_build_state(project)
        return

    # Initialize agent BEFORE rendering sidebar so sources are visible
    agent = initialize_agent(project)

    # Now render sidebar with agent populated
    render_sidebar(project)

    if st.session_state.app_status == "error" or agent is None:
        render_error_state()
        return

    # Get or create thread
    thread = get_or_create_thread()

    if thread is None:
        st.error("Failed to create conversation thread")
        return

    # Render main chat interface
    st.title("Databao")
    render_chat_interface(thread)


if __name__ == "__main__":
    main()
