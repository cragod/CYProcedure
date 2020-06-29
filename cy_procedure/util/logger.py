from datetime import datetime
from cy_components.helpers.formatter import DateFormatter
from cy_components.utils.notifier import MessageHandler, MessageType
from cy_widgets.logger.base import RecorderBase


class ProcedureRecorder(RecorderBase):

    def __init__(self, m_token: str, m_type: MessageType):
        self.__message_token = m_token
        self.__message_type = m_type
        self.__summary_log = ""

    def record_simple_info(self, content):
        print(content, end='\n\n')

    def record_procedure(self, content):
        content = DateFormatter.now_date_string('[%H:%M:%S] ') + content
        print(content, end='\n\n')

    def record_exception(self, content):
        print(content, end='\n\n')
        MessageHandler.send_message(content, 'Precedure Exception', self.__message_type, self.__message_token)

    def append_summary_log(self, content):
        # 打印并加入到最终日志
        # date_str = DateFormatter.convert_local_date_to_string(datetime.now(), '%H:%M:%S')
        self.record_simple_info(content)
        self.__summary_log = self.__summary_log + content + '\n\n'

    def record_summary_log(self, content=None):
        if content is not None:
            self.append_summary_log(content)
        MessageHandler.send_message(self.__summary_log, 'Procedure Summary', self.__message_type, self.__message_token)
