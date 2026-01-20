"""Databao Streamlit Web Interface - Main Application with Multipage Navigation."""

import logging
from pathlib import Path

import streamlit as st

from streamlit_app.models.chat_session import ChatSession

logger = logging.getLogger(__name__)

# Page config - use Databao logo as favicon
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
    # Chat sessions
    if "chats" not in st.session_state:
        st.session_state.chats = {}  # dict[str, ChatSession]
    if "current_chat_id" not in st.session_state:
        st.session_state.current_chat_id = None

    # DCE/Agent state
    if "dce_project" not in st.session_state:
        st.session_state.dce_project = None
    if "dce_project_path" not in st.session_state:
        st.session_state.dce_project_path = None  # Persists across reloads
    if "agent" not in st.session_state:
        st.session_state.agent = None
    if "app_status" not in st.session_state:
        st.session_state.app_status = "initializing"
    if "status_message" not in st.session_state:
        st.session_state.status_message = None
    if "executor_type" not in st.session_state:
        st.session_state.executor_type = "lighthouse"

    # Legacy state for compatibility with existing components
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "thread" not in st.session_state:
        st.session_state.thread = None

    # Suggestions state
    if "suggested_questions" not in st.session_state:
        st.session_state.suggested_questions = []
    if "suggestions_are_llm_generated" not in st.session_state:
        st.session_state.suggestions_are_llm_generated = False
    if "suggestions_status" not in st.session_state:
        st.session_state.suggestions_status = "not_started"
    if "suggestions_future" not in st.session_state:
        st.session_state.suggestions_future = None
    if "suggestions_cancel_event" not in st.session_state:
        st.session_state.suggestions_cancel_event = None

    # Title generation state
    if "title_futures" not in st.session_state:
        st.session_state.title_futures = {}


def _create_new_chat() -> None:
    """Create a new chat and navigate to it."""
    from uuid6 import uuid6

    # Save current chat's messages before creating new one
    prev_chat_id = st.session_state.get("current_chat_id")
    chats: dict[str, ChatSession] = st.session_state.get("chats", {})
    if prev_chat_id and prev_chat_id in chats:
        prev_chat = chats[prev_chat_id]
        prev_chat.messages = st.session_state.get("messages", [])
        prev_chat.thread = st.session_state.get("thread")

    # Create new chat
    chat_id = str(uuid6())
    chat = ChatSession(id=chat_id)

    chats[chat_id] = chat
    st.session_state.chats = chats
    st.session_state.current_chat_id = chat_id

    # Set up empty messages for the new chat
    st.session_state.messages = chat.messages
    st.session_state.thread = None

    # Update last synced chat ID
    st.session_state._last_synced_chat_id = chat_id

    # Flag to navigate to this chat on next rerun
    st.session_state._navigate_to_chat = chat_id


def build_navigation() -> None:
    """Build the multipage navigation structure."""
    from streamlit_app.pages.agent_settings import render_agent_settings_page
    from streamlit_app.pages.chat import render_chat_page
    from streamlit_app.pages.context_settings import render_context_settings_page
    from streamlit_app.pages.welcome import render_welcome_page

    # Check if we need to navigate to a newly created chat
    navigate_to_chat: str | None = st.session_state.get("_navigate_to_chat")
    if navigate_to_chat:
        # Clear the navigation flag
        st.session_state._navigate_to_chat = None

    # Settings pages - store in session state for page_link access
    context_settings_page = st.Page(
        render_context_settings_page,
        title="Context Settings",
        icon="📊",
        url_path="context-settings",
    )
    agent_settings_page = st.Page(
        render_agent_settings_page,
        title="Agent Settings",
        icon="⚙️",
        url_path="agent-settings",
    )
    settings_pages = [context_settings_page, agent_settings_page]

    # Store page objects in session state for cross-page navigation
    st.session_state._page_context_settings = context_settings_page
    st.session_state._page_agent_settings = agent_settings_page

    # Chat pages - build dynamically from session state
    chat_pages: list[st.Page] = []

    # "New Chat" action (using a function that creates and navigates)
    def new_chat_action():
        _create_new_chat()
        # The chat is created and _navigate_to_chat is set.
        # We need to rerun so navigation picks up the new chat.
        st.rerun()

    chat_pages.append(
        st.Page(
            new_chat_action,
            title="New Chat",
            icon=":material/add:",
            url_path="new-chat",
        )
    )

    # Existing chats
    chats: dict[str, ChatSession] = st.session_state.get("chats", {})
    target_chat_page: st.Page | None = None

    if chats:
        # Sort by creation time, newest first
        sorted_chats = sorted(chats.values(), key=lambda c: c.created_at, reverse=True)

        for chat in sorted_chats:
            # Create a page for each chat
            # Use a closure to capture the chat_id
            def make_chat_page(chat_id: str):
                def page_fn():
                    st.session_state.current_chat_id = chat_id
                    # Sync messages with this chat
                    _sync_chat_messages(chat_id)
                    render_chat_page()

                return page_fn

            title = chat.display_title

            # Check if this is the chat we should navigate to
            is_target = navigate_to_chat == chat.id

            page = st.Page(
                make_chat_page(chat.id),
                title=title,
                icon="💬",
                url_path=f"chat-{chat.id}",  # Flat path (no nested paths allowed)
                default=is_target,  # Make this the default if we're navigating to it
            )
            chat_pages.append(page)

            if is_target:
                target_chat_page = page

    # Welcome page (default only if we're not navigating to a chat)
    welcome_page = st.Page(
        render_welcome_page,
        title="Home",
        icon="🏠",
        url_path="welcome",
        default=(navigate_to_chat is None),
    )

    # Store welcome page in session state for navigation
    st.session_state._page_welcome = welcome_page

    # Build navigation with sections
    pages = {
        "": [welcome_page],  # Empty string = no header
        "Settings": settings_pages,
        "Chats": chat_pages,
    }

    pg = st.navigation(pages)

    # If we have a target chat page and navigation returned it, switch to it
    if target_chat_page is not None:
        st.switch_page(target_chat_page)

    pg.run()


def _sync_chat_messages(chat_id: str) -> None:
    """Sync session state messages when switching between chats.

    This saves the current chat's messages and loads the new chat's messages.
    Messages are stored in-memory in the ChatSession objects.
    """
    chats: dict[str, ChatSession] = st.session_state.get("chats", {})
    chat = chats.get(chat_id)

    if chat is None:
        return

    # Check if we're actually switching chats
    prev_chat_id = st.session_state.get("_last_synced_chat_id")
    if prev_chat_id == chat_id:
        # Same chat, no switch needed
        return

    # Save current messages to previous chat before switching
    if prev_chat_id and prev_chat_id in chats:
        prev_chat = chats[prev_chat_id]
        # Directly assign the list (not a copy) - the ChatSession holds the reference
        prev_chat.messages = st.session_state.messages
        prev_chat.thread = st.session_state.get("thread")

    # Load new chat's messages (direct assignment, ChatSession holds the reference)
    st.session_state.messages = chat.messages
    st.session_state.thread = chat.thread
    st.session_state._last_synced_chat_id = chat_id


def _render_global_sidebar() -> None:
    """Render sidebar elements that appear on all pages."""
    from streamlit_app.components.sidebar import render_sidebar_header

    with st.sidebar:
        render_sidebar_header()


def main() -> None:
    """Main application entry point."""
    init_session_state()
    _render_global_sidebar()
    build_navigation()


if __name__ == "__main__":
    main()
