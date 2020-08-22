from datetime import datetime
from cy_components.helpers.formatter import DateFormatter
from cy_components.utils.notifier import MessageHandler, MessageType
from cy_widgets.logger.base import RecorderBase


class ProcedureRecorder(RecorderBase):

    def __init__(self, m_token: str, m_type: MessageType):
        self.__message_token = m_token
        self.__message_type = m_type

    def record_simple_info(self, content):
        print(content, end='\n\n')

    def record_procedure(self, content):
        content = DateFormatter.now_date_string('[%H:%M:%S] ') + content
        print(content, end='\n\n')

    def record_exception(self, content):
        print(content, end='\n\n')
        MessageHandler.send_message(content, 'Precedure Exception', self.__message_type, self.__message_token)

    def _record_summary_log(self):
        MessageHandler.send_message(self._summary_log, 'Procedure Summary', self.__message_type, self.__message_token)
