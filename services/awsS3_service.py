# -*- coding: utf-8 -*-
import boto3, json
import botocore, subprocess
import constants
from models.lp_exception import LPException
from services.singleton_service import SingletonService
import re


class AwsS3Service(SingletonService):
    def __init__(self, logger, mode: str):
        self.logger = logger
        if mode == constants.env["development"]:
            self.bucket = "pay-stg"
            self.profile = "pay-stg"
        elif mode == constants.env["staging"]:
            self.bucket = "pay-stg"
            self.profile = "pay-stg"
        elif mode == constants.env["staging-aws"]:
            self.bucket = "pay-stg"
            self.profile = "pay-stg"
        elif mode == constants.env["production"]:
            self.bucket = "pay-prd"
            self.profile = "pay-prd"
        elif mode == constants.env["production-aws"]:
            self.bucket = "pay-prd"
            self.profile = "pay-prd"

        session = boto3.Session(profile_name=self.profile)
        self.s3_client = session.client("s3")
        self.s3_resource = session.resource("s3")

    def file_exists(self, filename: str) -> bool:
        try:
            result = self.s3_client.list_objects(Bucket=self.bucket, Prefix=filename)[
                "Contents"
            ]
            if len(result) > 0:
                return True
            else:
                return False
        except botocore.exceptions.ClientError as e:  # type: ignore
            if e.response["Error"]["Code"] == "404":
                self.logger.debug(e.response)
                return False
            else:
                raise LPException(self.logger, "AwsS3Service.file_exists", f"{e}")
        except KeyError as e:
            return False

    def download_file(self, file_from: str, save_to: str) -> None:
        if self.file_exists(file_from):
            try:
                self.s3_resource.Bucket(self.bucket).download_file(file_from, save_to)  # type: ignore
            except botocore.exceptions.ClientError as e:  # type: ignore
                self.logger.error(
                    "[Error] something wrong with download: {} ".format(
                        e.response["Error"]
                    )
                )
                raise LPException(self.logger, "AwsS3Service.download_file", f"{e}")
            self.logger.debug({"status": True, "msg": "saved to {}".format(save_to)})
        else:
            raise LPException(
                self.logger,
                "AwsS3Service.download_file",
                "file : {} is not exists".format(file_from),
            )

    def upload_file(self, file_from: str, save_to: str):
        try:
            self.s3_resource.Bucket(self.bucket).upload_file(file_from, save_to)  # type: ignore
        except botocore.exceptions.ClientError as e:  # type: ignore
            raise LPException(self.logger, "AwsS3Service.upload_file", f"{e}")
        self.logger.debug({"status": True, "msg": "saved to {}".format(save_to)})

    def copy_file(self, file_from, save_to):
        copy_source = {"Bucket": self.bucket, "Key": file_from}
        try:
            self.s3_resource.Bucket(self.bucket).copy(copy_source, save_to)  # type: ignore
        except botocore.exceptions.ClientError as e:  # type: ignore
            raise LPException(self.logger, "AwsS3Service.copy_file", f"{e}")
        self.logger.debug({"status": True, "msg": "saved to {}".format(save_to)})

    def bulk_download_file(self, folder: str, save_to: str) -> None:
        self.logger.info("from:[{}] to:[{}] ダウンロード開始".format(folder, save_to))
        s3_url = "s3://{}/{}".format(self.bucket, folder)
        cmd = [
            "aws",
            "--profile",
            self.profile,
            "s3",
            "cp",
            s3_url,
            save_to,
            "--recursive",
        ]
        result = subprocess.call(
            cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        self.logger.info("ダウンロード完了")

    def bulk_upload_files(self, folder: str, save_to: str) -> None:
        self.logger.info("from:[{}] to:[{}] アップロード開始".format(folder, save_to))
        s3_url = "s3://{}/{}".format(self.bucket, save_to)
        cmd = [
            "aws",
            "--profile",
            self.profile,
            "s3",
            "cp",
            folder,
            s3_url,
            "--recursive",
        ]
        result = subprocess.call(
            cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        self.logger.info("アップロード完了")

    def folder_exists(self, folder):
        keys = self.get_all_keys(folder)
        if len(keys) <= 0:
            return False
        else:
            return True

    def get_all_keys(self, folder):
        next_token = ""
        keys = []
        while True:
            if next_token == "":
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket, Prefix=folder
                )
            else:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket, Prefix=folder, ContinuationToken=next_token
                )
            if "Contents" not in response:
                break
            for content in response["Contents"]:
                keyn = content["Key"]
                prt = keyn.split("/")
                keys.append(prt)
            if "NextContinuationToken" in response:
                next_token = response["NextContinuationToken"]
            else:
                break
        return keys

    def bulk_download_file_from_tool_bucket(self, folder: str, save_to: str) -> None:
        self.logger.info("from:[{}] to:[{}] ダウンロード開始".format(folder, save_to))
        s3_url = "s3://{}/{}".format("tool-reporting", folder)
        cmd = [
            "aws",
            "--profile",
            self.profile,
            "s3",
            "cp",
            s3_url,
            save_to,
            "--recursive",
        ]
        result = subprocess.call(
            cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        self.logger.info("ダウンロード完了")

    def upload_file_to_tool_bucket(self, file_from: str, save_to: str):
        try:
            self.s3_resource.Bucket("tool-reporting").upload_file(file_from, save_to)  # type: ignore
        except botocore.exceptions.ClientError as e:  # type: ignore
            raise LPException(self.logger, "AwsS3Service.upload_file", f"{e}")
        self.logger.debug({"status": True, "msg": "saved to {}".format(save_to)})

    def search_files_in_folder(self, folder, file_name):
        next_token = ""
        files = []
        while True:
            if next_token == "":
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket, Prefix=folder
                )
            else:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket, Prefix=folder, ContinuationToken=next_token
                )
            if "Contents" not in response:
                break
            filted_keys = []
            if isinstance(file_name, re.Pattern):
                filted_keys = list(
                    filter(lambda x: file_name.search(x["Key"]), response["Contents"])
                )
            else:
                filted_keys = list(
                    filter(lambda x: file_name in x["Key"], response["Contents"])
                )
            for filted_key in filted_keys:
                files.append(filted_key["Key"])
            if "NextContinuationToken" in response:
                next_token = response["NextContinuationToken"]
            else:
                break
        return files

    def delete_file(self, file):
        if self.file_exists(file):
            try:
                self.s3_client.delete_object(Bucket=self.bucket, Key=file)
            except botocore.exceptions.ClientError as e:  # type: ignore
                self.logger.error(
                    "[Error] something wrong with download: {} ".format(
                        e.response["Error"]
                    )
                )
                raise LPException(self.logger, "AwsS3Service.delete_file", f"{e}")
        else:
            raise LPException(
                self.logger,
                "AwsS3Service.delete_file",
                "file : {} is not exists".format(file),
            )

    def delete_folder(self, folder):
        next_token = ""
        keys = []
        while True:
            if next_token == "":
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket, Prefix=folder
                )
            else:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket, Prefix=folder, ContinuationToken=next_token
                )
            if "Contents" not in response:
                break
            for content in response["Contents"]:
                self.s3_client.delete_object(Bucket=self.bucket, Key=content["Key"])
            if "NextContinuationToken" in response:
                next_token = response["NextContinuationToken"]
            else:
                break
        return keys
