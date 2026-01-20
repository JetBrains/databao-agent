"""Chat interface component with streaming support."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

import streamlit as st

from streamlit_app.components.results import render_execution_result
from streamlit_app.streaming import StreamingWriter
from streamlit_app.suggestions import (
    check_suggestions_completion,
    is_suggestions_loading,
    start_suggestions_generation,
)

if TYPE_CHECKING:
    from databao.core.thread import Thread


@dataclass
class ChatMessage:
    """Represents a chat message."""

    role: str  # "user" or "assistant"
    content: str
    thinking: str | None = None
    result: Any | None = None  # ExecutionResult
    has_visualization: bool = False
    message_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)


def render_user_message(message: ChatMessage) -> None:
    """Render a user message."""
    with st.chat_message("user"):
        st.markdown(message.content)


def render_assistant_message(
    message: ChatMessage, thread: "Thread", message_index: int, *, is_latest: bool = False
) -> None:
    """Render an assistant message with results."""
    with st.chat_message("assistant"):
        # Render thinking section (collapsed)
        if message.thinking:
            with st.expander("💭 Thinking", expanded=False):
                st.code(message.thinking, language=None)

        # Render execution result
        if message.result:
            render_execution_result(
                result=message.result,
                thread=thread,
                message_index=message_index,
                has_visualization=message.has_visualization,
                is_latest=is_latest,
            )


def _truncate_question(question: str, max_len: int = 60) -> tuple[str, bool]:
    """Truncate a question for display, returning (display_text, was_truncated)."""
    if len(question) <= max_len:
        return question, False
    return question[: max_len - 3] + "...", True


@st.fragment
def render_welcome_component() -> None:
    """Render the welcome component with greeting and suggested questions.

    This is a fragment so it has an independent render lifecycle from the main page.
    When the main page reruns for processing, this fragment can be cleanly removed
    without showing stale elements.

    It handles these states:
    - not_started: starts background generation, shows loading
    - loading: shows "Analyzing your data..." with no buttons (polls for completion)
    - ready: shows questions with appropriate subtitle
    """
    # Get current state
    status = st.session_state.get("suggestions_status", "not_started")

    # Start background generation if not started
    if status == "not_started":
        agent = st.session_state.get("agent")
        if agent is not None:
            start_suggestions_generation(agent)
            status = "loading"

    # Create centered container with vertical spacing
    st.markdown("<div style='height: 15vh'></div>", unsafe_allow_html=True)

    # Greeting message (always shown)
    st.markdown(
        """
        <div style='text-align: center;'>
            <h2>Welcome to Databao!</h2>
            <p style='color: gray; font-size: 1.1em;'>
                Ask questions about your data and get instant insights with tables and visualizations.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if status == "loading":
        # Show loading UI - no buttons yet
        st.markdown(
            "<p style='text-align: center; color: #888; font-size: 0.9em; margin-top: 1em;'>"
            "🔄 Analyzing your data to suggest questions..."
            "</p>",
            unsafe_allow_html=True,
        )
        return

    # Ready state (or cancelled - which now falls back to showing suggestions)
    questions: list[str] = st.session_state.get("suggested_questions", [])
    is_llm_generated: bool = st.session_state.get("suggestions_are_llm_generated", False)

    # Show appropriate subtitle
    if questions:
        if is_llm_generated:
            st.markdown(
                "<p style='text-align: center; color: #888; font-size: 0.9em; margin-top: 1em;'>"
                "✨ These questions were generated based on your data"
                "</p>",
                unsafe_allow_html=True,
            )
        else:
            # Fallback questions - soft message, no error indication
            st.markdown(
                "<p style='text-align: center; color: #888; font-size: 0.9em; margin-top: 1em;'>"
                "Try these questions to get started"
                "</p>",
                unsafe_allow_html=True,
            )

        st.markdown("<div style='height: 2em'></div>", unsafe_allow_html=True)

        # Question buttons with truncation and hover expansion
        cols = st.columns(len(questions))
        for i, (col, question) in enumerate(zip(cols, questions, strict=True)):
            with col:
                display_text, was_truncated = _truncate_question(question)

                # Show truncated text in button, but use help tooltip for full text if truncated
                help_text = question if was_truncated else None
                if st.button(display_text, key=f"suggested_q_{i}", width="stretch", help=help_text):
                    # Submit the FULL question (not truncated)
                    # Appending directly updates ChatSession.messages (via reference)
                    user_message = ChatMessage(role="user", content=question)
                    st.session_state.messages.append(user_message)
                    st.session_state.pending_query = question
                    # Use scope="app" to rerun entire app, not just this fragment
                    st.rerun(scope="app")
    else:
        # No questions available (edge case) - show generic message
        st.markdown(
            "<p style='text-align: center; color: #888; font-size: 0.9em; margin-top: 1em;'>"
            "Type your question below to get started"
            "</p>",
            unsafe_allow_html=True,
        )


def render_chat_history(thread: "Thread") -> None:
    """Render all messages in chat history."""
    messages: list[ChatMessage] = st.session_state.messages

    # Check if we're currently processing a query - if so, hide all buttons
    is_processing = st.session_state.get("pending_query") is not None

    # Find the index of the last assistant message
    last_assistant_idx = -1
    for i, msg in enumerate(messages):
        if msg.role == "assistant":
            last_assistant_idx = i

    for i, message in enumerate(messages):
        if message.role == "user":
            render_user_message(message)
        else:
            # Only show buttons on latest message AND when not processing
            is_latest = (i == last_assistant_idx) and not is_processing
            render_assistant_message(message, thread, i, is_latest=is_latest)


def process_pending_query(thread: "Thread") -> None:
    """Process the pending query from session state."""
    user_input = st.session_state.pending_query

    # Generate assistant response
    with st.chat_message("assistant"):
        # Get the streaming writer from session state
        writer: StreamingWriter | None = st.session_state.get("streaming_writer")
        if writer:
            writer.clear()  # Clear any previous content

        # Show thinking expander while streaming
        thinking_placeholder = st.empty()

        with thinking_placeholder.container():
            thinking_expander = st.expander("💭 Thinking...", expanded=True)

            with thinking_expander:
                thinking_display = st.empty()

                # Execute with streaming
                try:
                    # Set up real-time update callback if writer exists
                    if writer:
                        writer._on_write = lambda text: thinking_display.code(text, language=None)

                    # Execute the query - output goes to writer automatically
                    thread.ask(user_input, stream=True)

                    # Get the result
                    result = thread._data_result

                except Exception as e:
                    st.error(f"Error: {e}")
                    # Add error message (appending directly updates ChatSession.messages)
                    error_message = ChatMessage(
                        role="assistant",
                        content=f"Error processing request: {e}",
                    )
                    st.session_state.messages.append(error_message)
                    st.session_state.pending_query = None
                    st.rerun()
                    return

        # Clear the thinking placeholder
        thinking_placeholder.empty()

        # Get captured thinking text
        thinking_text = writer.getvalue() if writer else ""

        # Check if visualization was generated
        has_visualization = False
        if result and result.meta:
            hints = result.meta.get("output_modality_hints")
            if hints:
                has_visualization = getattr(hints, "should_visualize", False)

        # Also check if visualization_result exists
        if thread._visualization_result is not None:
            has_visualization = True

        # Create the final message and add to session state
        # Appending directly updates ChatSession.messages (via reference)
        assistant_message = ChatMessage(
            role="assistant",
            content=result.text if result else "",
            thinking=thinking_text,
            result=result,
            has_visualization=has_visualization,
        )
        st.session_state.messages.append(assistant_message)

        # Clear pending query and rerun to show final state
        st.session_state.pending_query = None
        st.rerun()


@st.fragment(run_every=1.0)
def _suggestions_polling_fragment() -> None:
    """Fragment that polls for suggestions completion every second.

    This runs independently from the main app, checking if the background
    suggestions generation has completed. When it completes, triggers a rerun
    to show the suggestions.

    The fragment automatically stops polling when suggestions are no longer loading
    (either completed, cancelled, or never started).
    """
    # Only poll if we're in loading state - this makes the fragment a no-op
    # once suggestions are ready, effectively stopping the polling
    if not is_suggestions_loading():
        return

    # Check if background task completed
    if check_suggestions_completion():
        # Suggestions are ready - rerun the full app to show them
        st.rerun()


def _should_show_welcome() -> bool:
    """Determine if we should show the welcome screen.

    Returns False if:
    - There are any messages in the chat
    - There's a pending query being processed
    """
    # Check directly from session_state for most up-to-date values
    has_messages = len(st.session_state.get("messages", [])) > 0
    has_pending = st.session_state.get("pending_query") is not None
    return not has_messages and not has_pending


def render_chat_interface(thread: "Thread") -> None:
    """Render the complete chat interface."""
    # Chat input at the bottom (always rendered, disabled while processing)
    is_processing = st.session_state.get("pending_query") is not None
    user_input = st.chat_input("Ask a question about your data...", disabled=is_processing)

    # Handle user input FIRST, before rendering main content
    if user_input:
        # Add user message immediately
        # Appending directly updates ChatSession.messages (via reference)
        user_message = ChatMessage(role="user", content=user_input)
        st.session_state.messages.append(user_message)

        # Set pending query - the rerun will show the chat with user message
        st.session_state.pending_query = user_input
        st.rerun()

    # Determine what to render
    show_welcome = _should_show_welcome()

    if show_welcome:
        render_welcome_component()

        # Add polling fragment for suggestions (only active while loading)
        if is_suggestions_loading():
            _suggestions_polling_fragment()
    else:
        # Render chat history (buttons hidden if pending_query is set)
        render_chat_history(thread)

        # Process pending query after showing the user message
        if is_processing:
            process_pending_query(thread)
