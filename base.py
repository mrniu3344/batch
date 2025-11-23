import logging.config
import logging.handlers
import os
import argparse
import constants as constants


def getFileNameFromPath(filePath):
    if filePath is None:
        return "out"
    return os.path.splitext(os.path.basename(filePath))[0]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-m",
        required=True,
        type=str,
        choices=[
            constants.env["development"],
            constants.env["staging"],
            constants.env["staging-aws"],
            constants.env["production"],
            constants.env["production-aws"],
        ],
    )
    parser.add_argument("-n", "--appName", dest="appName", help="app name")
    return parser.parse_args()


def parse_args_c():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-m",
        required=True,
        type=str,
        choices=[
            constants.env["development"],
            constants.env["staging"],
            constants.env["staging-aws"],
            constants.env["production"],
            constants.env["production-aws"],
        ],
    )
    parser.add_argument("-n", "--appName", dest="appName", help="app name")
    parser.add_argument("-c", required=True, type=str, choices=["cpu", "gpu"])
    return parser.parse_args()


def getLogger(mod, filePath):
    if mod == "prd-aws":
        mod = "prd"
    elif mod == "stg-aws":
        mod = "stg"
    logging.config.fileConfig(f"configs/logging.{mod}.conf")
    logger = logging.getLogger()
    return logger
