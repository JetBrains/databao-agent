"""Chat interface component with streaming support."""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import streamlit as st

from streamlit_app.components.results import render_execution_result
from streamlit_app.streaming import StreamingWriter

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
                    # Add error message
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


def render_chat_interface(thread: "Thread") -> None:
    """Render the complete chat interface."""
    is_processing = st.session_state.get("pending_query") is not None

    # Render chat history (buttons hidden if pending_query is set)
    render_chat_history(thread)

    # Check if there's a pending query to process
    if is_processing:
        process_pending_query(thread)
        return

    # Chat input (disabled while processing to prevent double submission)
    if user_input := st.chat_input("Ask a question about your data...", disabled=is_processing):
        # Add user message immediately
        user_message = ChatMessage(role="user", content=user_input)
        st.session_state.messages.append(user_message)

        # Set pending query and rerun - this hides buttons immediately
        st.session_state.pending_query = user_input
        st.rerun()
