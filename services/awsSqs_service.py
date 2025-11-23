# -*- coding: utf-8 -*-
import boto3
import json
import constants


class AwsSqsService:
    def __init__(self, logger, mode: str):
        self.logger = logger
        session = boto3.Session(profile_name=self.profile)
        if mode == constants.env["development"]:
            self.profile = "pay-stg"
            self.sqs = session.resource("sqs")
        elif mode == constants.env["staging"]:
            self.profile = "pay-stg"
            self.sqs = session.resource("sqs")
        elif mode == constants.env["staging-aws"]:
            self.profile = "pay-stg"
            self.sqs = session.resource("sqs")
        elif mode == constants.env["production"]:
            self.profile = "pay-prd"
            self.sqs = session.resource("sqs")
        elif mode == constants.env["production-aws"]:
            self.profile = "pay-prd"
            self.sqs = session.resource("sqs")

    def getMessage(self, q_name, MaxNumberOfMessages=1):
        self.logger.debug("QueueName = %s" % q_name)
        queue = self.sqs.get_queue_by_name(QueueName=q_name)  # type: ignore
        ret = []
        # メッセージを取得
        msg_list = queue.receive_messages(MaxNumberOfMessages=MaxNumberOfMessages)
        if msg_list:
            for message in msg_list:
                self.logger.debug("getMessage = %s" % message.body)
                if "end" in json.loads(message.body.replace("'", "")):
                    self.logger.debug("receive end message")
                    message.delete()
                    return
                message.delete()
                ret.append(json.loads(message.body.replace("'", "")))
        return ret

    def putMessage(self, q_name, strMsg):
        self.logger.debug("QueueName = %s" % q_name)
        queue = self.sqs.get_queue_by_name(QueueName=q_name)  # type: ignore
        strMsg = json.dumps(strMsg)
        self.logger.debug("putMessage = %s" % strMsg)
        response = queue.send_message(MessageBody=strMsg)
        self.logger.debug(response)
