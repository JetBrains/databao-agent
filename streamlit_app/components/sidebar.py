"""Sidebar component showing project info and sources."""

import streamlit as st

from databao.dce import DCEProject, DCEProjectStatus
from streamlit_app.suggestions import reset_suggestions_state

# Icons for different database types
DB_ICONS = {
    "duckdb": "🦆",
    "postgres": "🐘",
    "postgresql": "🐘",
    "mysql": "🐬",
    "sqlite": "📦",
    "default": "🗄️",
}

# Available executor types
EXECUTOR_TYPES = {
    "lighthouse": "LighthouseExecutor (recommended)",
    "react_duckdb": "ReactDuckDBExecutor (experimental)",
}


def get_db_icon(db_type: str) -> str:
    """Get icon for database type."""
    return DB_ICONS.get(db_type.lower(), DB_ICONS["default"])


def render_project_info(project: DCEProject | None) -> None:
    """Render project information section."""
    st.markdown("### 📊 Project")

    if project is None:
        st.caption("No project selected")
        return

    st.markdown(f"**{project.name}**")
    st.caption(str(project.path))

    # Status indicator
    if project.status == DCEProjectStatus.VALID:
        st.success("✓ Ready", icon="✅")
        if project.latest_run:
            st.caption(f"Build: {project.latest_run}")
    elif project.status == DCEProjectStatus.NO_BUILD:
        st.warning("Build required", icon="⚠️")
    else:
        st.error("Not found", icon="❌")


def render_sources_info() -> None:
    """Render connected sources section."""
    st.markdown("### 🔗 Sources")

    agent = st.session_state.get("agent")
    if agent is None:
        st.caption("No sources connected")
        return

    # Get databases
    dbs = agent.dbs
    dfs = agent.dfs

    if not dbs and not dfs:
        st.caption("No sources configured")
        return

    # List databases
    for name, source in dbs.items():
        # Try to determine DB type from connection
        conn = source.db_connection
        db_type = type(conn).__name__
        if "duckdb" in db_type.lower():
            icon = get_db_icon("duckdb")
            db_type = "DuckDB"
        elif "engine" in db_type.lower():
            # SQLAlchemy engine - try to get dialect
            try:
                dialect = conn.dialect.name
                icon = get_db_icon(dialect)
                db_type = dialect.capitalize()
            except Exception:
                icon = get_db_icon("default")
                db_type = "Database"
        else:
            icon = get_db_icon("default")

        st.markdown(f"{icon} **{name}** ({db_type})")

    # List dataframes
    for name in dfs:
        st.markdown(f"📊 **{name}** (DataFrame)")


def render_status_info() -> None:
    """Render app status section."""
    status = st.session_state.get("app_status", "initializing")

    if status == "ready":
        pass  # Don't show anything when ready
    elif status == "initializing":
        st.info("Initializing...", icon="⏳")
    elif status == "error":
        error_msg = st.session_state.get("error_message", "Unknown error")
        st.error(error_msg, icon="❌")


def render_executor_selector() -> None:
    """Render executor type selector."""
    st.markdown("### ⚙️ Executor")

    current = st.session_state.get("executor_type", "lighthouse")

    selected = st.selectbox(
        "Executor type",
        options=list(EXECUTOR_TYPES.keys()),
        index=list(EXECUTOR_TYPES.keys()).index(current),
        format_func=lambda x: EXECUTOR_TYPES[x],
        label_visibility="collapsed",
        help="Choose the execution engine for queries",
    )

    if selected != current:
        st.session_state.executor_type = selected
        # Reset agent when executor changes
        st.session_state.agent = None
        st.session_state.thread = None
        st.session_state.messages = []
        st.session_state.app_status = "initializing"
        st.rerun()


def render_actions() -> None:
    """Render action buttons."""
    st.markdown("### ⚡ Actions")

    col1, col2 = st.columns(2)

    with col1:
        if st.button("🔄 Reload", width="stretch", help="Reload DCE project"):
            st.session_state.dce_project = None
            st.session_state.agent = None
            st.session_state.thread = None
            st.session_state.messages = []
            st.session_state.app_status = "initializing"
            # Reset suggestions so they get regenerated with new agent
            reset_suggestions_state()
            st.rerun()

    with col2:
        if st.button("🗑️ Clear", width="stretch", help="Clear chat history"):
            st.session_state.messages = []
            # Create new thread to reset conversation
            if st.session_state.agent:
                st.session_state.thread = st.session_state.agent.thread(stream_ask=True, stream_plot=False)
            # Reset suggestions so user sees fresh suggestions on welcome screen
            reset_suggestions_state()
            st.rerun()


def render_sidebar(project: DCEProject | None) -> None:
    """Render the complete sidebar."""
    import base64
    from pathlib import Path

    with st.sidebar:
        # Header with logo - use HTML for proper vertical alignment
        logo_path = Path(__file__).parent.parent / "assets" / "bao.png"
        if logo_path.exists():
            with open(logo_path, "rb") as f:
                logo_b64 = base64.b64encode(f.read()).decode()
            st.markdown(
                f"""
                <div style="display: flex; align-items: center; gap: 6px;">
                    <img src="data:image/png;base64,{logo_b64}" width="32" height="32" style="vertical-align: middle;">
                    <span style="font-size: 1.4rem; font-weight: 600; line-height: 32px;">Databao</span>
                </div>
                """,
                unsafe_allow_html=True,
            )
        else:
            st.markdown("## Databao")
        st.markdown("---")

        # Project info
        render_project_info(project)

        st.markdown("---")

        # Sources
        render_sources_info()

        st.markdown("---")

        # Executor selector
        render_executor_selector()

        st.markdown("---")

        # Status
        render_status_info()

        # Actions
        render_actions()

        # Footer
        st.markdown("---")
        st.caption("Databao v0.1")
