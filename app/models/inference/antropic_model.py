import os

from langchain_anthropic import ChatAnthropic as BaseChatAnthropic


class ChatAnthropic(BaseChatAnthropic):
    """A wrapper for the `langchain_aws.ChatBedrock`."""

    def __init__(self, **kwargs):
        """Initialize the `ChatBedrock` with specific configuration."""
        model_type, model_id = os.environ['LLM_MODEL_ID'].split(':', 1)
        default_kwargs = {
            'model': model_id,
            'temperature': 0,
            'request_timeout': 30
        }

        super().__init__(**(default_kwargs | kwargs))
