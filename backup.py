#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: set tabstop=4:softtabstop=4:shiftwidth=4:expandtab:textwidth=120

"""
This script aims to provide an easy way to configure backups

This script should be compatible with python 2 and python 3
"""

__author__ = "Samuel Déal"
__copyright__ = "Copyright 2019, Aziugo SAS"
__credits__ = ["Samuel Déal"]
__license__ = "GPL"
__version__ = "0.9.0"
__maintainer__ = "Samuel Déal"
__status__ = "Development"


# Python core libs
import sys
import os
import argparse
import collections
import copy
import re
import logging
import logging.handlers
import signal
import datetime
import contextlib
import tempfile
import subprocess
import platform
import json
import email.utils
import smtplib
import email.mime.text
import syslog
import glob
import importlib
import fnmatch
import math
import locale


script_path = os.path.dirname(os.path.realpath(os.path.abspath(__file__)))
log = logging.getLogger("backuper")

logging.getLogger('boto').setLevel(logging.ERROR)
logging.getLogger('boto3').setLevel(logging.ERROR)
logging.getLogger('botocore').setLevel(logging.ERROR)
logging.getLogger('s3transfer').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)


# Python 2/3 compatibility mapping
# ----------------------------------------------------------------------------
if sys.version_info[0] == 2:  # Python 2 version
    import ConfigParser
    configparser = ConfigParser

    def is_string(var):
        """
        Check if the var is a string

        :param var:     The variable to test
        :type var:      any
        :return:        True if the variable is a kind of string
        :rtype:         bool
        """
        return isinstance(var, basestring)

    def is_primitive(var):
        """
        Check if the var is a primitive (aka scalar) var (like string, int, float, etc...)

        :param var:     The variable to test
        :type var:      any
        :return:        True if the variable's type is a primitive type
        :rtype:         bool
        """
        return isinstance(var, (basestring, int, long, bool, float))

    def read_stdin(question_str):
        return unicode(raw_input(question_str)).strip()

    def to_bytes(var):
        """
        Convert a var to a byte string

        :param var:     The variable to convert
        :type var:      any
        :return:        The var converted to byte string
        :rtype:         str
        """
        if isinstance(var, str):
            return var
        elif isinstance(var, unicode):
            return var.encode("UTF-8")
        else:
            return str(var)

    def to_unicode(var):
        """
        Convert a var to a unicode string

        :param var:     The variable to convert
        :type var:      any
        :return:        The var converted to string
        :rtype:         unicode
        """
        if isinstance(var, unicode):
            return var
        elif isinstance(var, str):
            return var.decode("UTF-8")
        else:
            return str(var).decode("UTF-8")

    def to_str(var):
        """
        Convert a var to a string

        :param var:     The variable to convert
        :type var:      any
        :return:        The var converted to string
        :rtype:         str
        """
        return to_bytes(var)

    def first(dict_var):
        """
        Get first key of a dictionary

        :param dict_var:    A non empty dictionary
        :type dict_var:     dict[any, any]
        :return:            The first key and value of the input var
        :rtype:             tuple[any, any]
        """
        return dict_var.iteritems().next()

    def shell_quote(arg):
        """
        Quote a parameter for shell usage
        Example:
            shell_quote("it's a cool weather today") => 'it'"'"'s a cool weather today'

        :param arg:     The argument to quote, required
        :type arg:      str|unicode
        :return:        The quoted argument
        :rtype:         str
        """
        import pipes
        return pipes.quote(arg)

else:  # Python 3 version
    import configparser

    StandardError = Exception

    def is_string(var):
        """
        Check if the var is a string

        :param var:     The variable to test
        :type var:      any
        :return:        True if the variable is a kind of string
        :rtype:         bool
        """
        return isinstance(var, str)

    def is_primitive(var):
        """
        Check if the var is a primitive (aka scalar) var (like string, int, float, etc...)

        :param var:     The variable to test
        :type var:      any
        :return:        True if the variable's type is a primitive type
        :rtype:         bool
        """
        return isinstance(var, (str, int, bool, float, bytes))

    def read_stdin(question_str):
        return input(question_str).strip()

    def to_bytes(var):
        """
        Convert a var to a byte string

        :param var:     The variable to convert
        :type var:      any
        :return:        The var converted to byte string
        :rtype:         bytes
        """
        if isinstance(var, bytes):
            return var
        elif isinstance(var, str):
            return var.encode("UTF-8")
        else:
            return str(var).encode("UTF-8")

    def to_unicode(var):
        """
        Convert a var to a unicode string

        :param var:     The variable to convert
        :type var:      any
        :return:        The var converted to string
        :rtype:         str
        """
        if isinstance(var, str):
            return var
        elif isinstance(var, bytes):
            return var.decode("UTF-8")
        else:
            return str(var)

    def to_str(var):
        """
        Convert a var to a string

        :param var:     The variable to convert
        :type var:      any
        :return:        The var converted to string
        :rtype:         str
        """
        return to_unicode(var)

    def first(dict_var):
        """
        Get first key of a dictionary

        :param dict_var:    A non empty dictionary
        :type dict_var:     dict[any, any]
        :return:            The first key and value of the input var
        :rtype:             tuple[any, any]
        """
        return next(iter(dict_var.items()))

    def shell_quote(arg):
        """
        Quote a parameter for shell usage
        Example:
            shell_quote("c'est cool aujourd'hui, il fait beau") => 'c'"'"'est cool aujourd'"'"'hui, il fait beau'

        :param arg:        String, the argument to quote, required
        :return:        String, the quoted argument
        """
        import shlex
        return shlex.quote(arg)


# Other utility definitions
# ----------------------------------------------------------------------------

def is_array(var):
    """
    Check if the input is a valid array, but not a string nor a dict

    :param var:     The input to test
    :type var:      any
    :return:        True if var is an array
    :rtype:         bool
    """
    if is_primitive(var):
        return False
    if is_dict(var):
        return False
    return isinstance(var, collections.Iterable)


def is_dict(var):
    """
    Check if the input is a valid dict

    :param var:     The input to test
    :type var:      any
    :return:        True if var is a dictionary
    :rtype:         bool
    """
    return isinstance(var, dict)


def ll_int(var):
    """
    Does the input looks like an int ?

    :param var:     The input to test
    :type var:      any
    :return:        True if the input looks like a valid int
    :rtype:         bool
    """
    try:
        int(var)
        return True
    except ValueError:
        return False


def ll_float(var):
    """
    Check parameter can be cast as a valid float

    :param var:     The variable to check
    :type var:      any
    :return:        True if the value can be cast to float
    :rtype:         bool
    """
    try:
        float(var)
        return True
    except (ValueError, TypeError):
        return False


def ll_bool(value):
    """
    Check if value looks like a bool

    :param value:       The value to check
    :type value:        any
    :return:            The boolean value
    :rtype:             bool
    """
    if value is None:
        return False
    if isinstance(value, bool):
        return True
    if ll_float(value):
        return int(float(value)) in (0, 1)
    try:
        value = str(value)
    except StandardError:
        return False
    if value.lower() in ('yes', 'true', 't', 'y', '1', 'o', 'oui', 'on'):
        return True
    elif value.lower() in ('no', 'false', 'f', 'n', '0', 'non', 'off'):
        return True
    else:
        return False


def to_bool(value):
    """
    Convert a string to bool
    Raise error if not a valid bool

    :param value:       The value to cast
    :type value:        any
    :return:            The boolean value
    :rtype:             bool
    """
    if value is None:
        raise TypeError("Not a boolean")
    if isinstance(value, bool):
        return value
    if ll_float(value):
        if not int(float(value)) in (0, 1):
            raise TypeError("Not a boolean")
        return int(float(value)) == 1
    try:
        value = str(value)
    except StandardError:
        raise TypeError("Not a boolean")
    if value.lower() in ('yes', 'true', 't', 'y', '1', 'o', 'oui', 'on'):
        return True
    elif value.lower() in ('no', 'false', 'f', 'n', '0', 'non', 'off'):
        return False
    else:
        raise TypeError("Not a boolean")


def indent(some_str=None, indent_first=True, indent_str="  "):
    """
    Indent a given input text

    :param some_str:        The input to indent. Optional, default None
    :type some_str:         str|None
    :param indent_first:    Do you want start by an indent or indent only the subsequent lines ? Optional, default True
    :type indent_first:     bool
    :param indent_str:      The string used as indentation. Optional default "  "
    :type indent_str:       str
    :return:                The indented text
    :rtype:                 str
    """
    if some_str is None:
        return indent_str
    if indent_first:
        return os.linesep.join([indent_str+line for line in to_str(some_str).splitlines()])
    else:
        return (os.linesep+indent_str).join([line for line in to_str(some_str).splitlines()])


def extract_keys(input_dict, *key_list):
    """
    Remove the keys of given dictionary and create a new dictionary with the extracted data

    :param input_dict:          A dictionary. WARNING: it will be modified
    :type input_dict:           dict[str, any]
    :param key_list:            the keys to extract
    :type key_list:             str
    :return:                    A sub dictionary with only the important keys
    :rtype:                     dict[str, any]
    """
    result = {}
    for key in key_list:
        if key in input_dict.keys():
            result[key] = input_dict[key]
            del input_dict[key]
    return result


def to_list(var):
    if is_array(var):
        return var
    else:
        return [var]


def index_by(array, key):
    """
    Reorganize given array inside a dict

    :param array:       A list to organize
    :type array:        list[any]
    :param key:         Key by which you want to organize your array
    :type key:          str|callable
    :return:            A dictionary with the key as index and a list as value
    :rtype:             dict[any, list[any]]
    """
    results = {}
    for element in array:
        if callable(key):
            new_key = key(element)
        else:
            new_key = getattr(element, key)
        if new_key not in results.keys():
            results[new_key] = []
        results[new_key].append(element)
    return results


def deep_merge(src, new):
    """

    :param src:
    :param new:
    :return:
    :rtype:         None|list|dict[any, any]|any
    """
    if new is None:
        return src
    if src is None:
        return new
    if is_array(src):
        result = copy.deepcopy(src)
        if is_array(new):
            result.extend(new)
        else:
            result.append(new)
        return result
    elif is_dict(src):
        if is_array(new):
            result = copy.deepcopy(new)
            result.append(src)
            return result
        if not is_dict(new):
            return [src, new]
        result = copy.deepcopy(src)
        for key, val in new.items():
            if key in result.keys():
                result[key] = deep_merge(result[key], val)
            else:
                result[key] = val
        return result
    return [src, new]


@contextlib.contextmanager
def using_cwd(path):
    """
    Change the current directory. Should be used using the `with` keyword.
    It yield and then restore the previous current directory

    :param path:    The path you want to use as new current directory
    :type path:     str
    """
    current_cwd = os.getcwd()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(current_cwd)


@contextlib.contextmanager
def temp_filename(suffix="", prefix="tmp", dir=None):
    """
    Generate a temporary file
    It yield the file path, and ensure file destruction

    :param suffix:      The end of the name of the generated temp file. Optional, default empty string
    :type suffix:       str
    :param prefix:      The beginning of the name of the generated temp file. Optional, default "tmp"
    :type prefix:       str
    :param dir:         The place where we will create the temporary file. Optional, default None
    :type dir:          str|None
    :return:            The temporary file path
    :rtype:             str
    """
    if dir and not os.path.exists(dir):
        os.makedirs(dir)
    tmp_filename = None
    fd = None
    try:
        fd, tmp_filename = tempfile.mkstemp(suffix, prefix, dir)
        yield tmp_filename
    finally:
        if fd is not None:
            os.close(fd)
        if tmp_filename is not None and os.path.exists(tmp_filename):
            os.remove(tmp_filename)


def which(exec_name):
    """
    Get path of an executable
    Raise error if not found

    :param exec_name:       The binary name
    :type exec_name:        str
    :return:                The path to the executable file
    :rtype:                 str
    """
    path = os.getenv('PATH')
    available_exts = ['']
    if "windows" in platform.system().lower():
        additional_exts = [ext.strip() for ext in os.getenv('PATHEXT').split(";")]
        available_exts += ["."+ext if ext[0] != "." else ext for ext in additional_exts]
    for folder in path.split(os.path.pathsep):
        for ext in available_exts:
            exec_path = os.path.join(folder, exec_name+ext)
            if os.path.exists(exec_path) and os.access(exec_path, os.X_OK):
                return exec_path
    raise RuntimeError("Unable to find path for executable "+str(exec_name))


class ConfigError(RuntimeError):
    pass


class CheckError(RuntimeError):
    pass


def init_log(log_output=None):
    """
    Initialize the logging of the application.
    It can configure standard stream outputs, syslog of file logging.

    :param log_output:  The output you want. stderr if None. Optional, default None
    :type log_output:   str|None
    """
    logging.getLogger().setLevel(logging.INFO)
    log.setLevel(logging.INFO)

    if log_output is None:
        log_output = "stderr"
    if log_output in ("stderr", "stdout"):
        log_file = sys.stderr if log_output == "stderr" else sys.stdout
        log_handler = logging.StreamHandler(stream=log_file)
        log_handler.setFormatter(logging.Formatter('%(message)s'))
    elif log_output == "syslog":
        log_handler = logging.handlers.SysLogHandler(address='/dev/log')
        log_handler.setFormatter(logging.Formatter('%(name)s[%(process)d]: %(levelname)s %(message)s'))
    else:
        log_handler = logging.FileHandler(log_output)
        log_handler.setFormatter(logging.Formatter('%(asctime)s: %(levelname)-7s: %(name)s - %(message)s'))
    logging.getLogger().addHandler(log_handler)
    log.addHandler(log_handler)
    log.propagate = False


def flush_loggers():
    loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
    for logger in loggers:
        for handler in logger.handlers:
            handler.flush()


def run_cmd(*cmd_args):
    if len(cmd_args) == 1 and is_array(cmd_args[0]):
        cmd_args = cmd_args[0]
    pipes = subprocess.Popen(cmd_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    std_out, std_err = pipes.communicate()
    return pipes.returncode, std_out.strip(), std_err.strip()


def check_run_cmd(*cmd_args):
    if len(cmd_args) == 1 and is_array(cmd_args[0]):
        cmd_args = cmd_args[0]
    code, out, err = run_cmd(*cmd_args)
    if code != 0:
        error = "Command failed with exit code "+to_str(code)+os.linesep
        error += "  Command: "+" ".join([shell_quote(arg) for arg in cmd_args])
        if err:
            error += os.linesep+"  Error output:"+os.linesep+indent(to_str(err), indent_str="    ")
        raise RuntimeError(error)
    return out


class KillEventHandler(object):
    """ Static class used to force killing of the script if too many and quit signals are received."""
    INTERVAL = datetime.timedelta(seconds=1)
    EVENT_COUNT = 3

    _quit_msg = None

    @staticmethod
    def initialise():
        """ Start to listen to system signals """
        if KillEventHandler._quit_msg is not None:
            return
        KillEventHandler._quit_msg = []
        signal.signal(signal.SIGINT, KillEventHandler._on_signal)
        signal.signal(signal.SIGTERM, KillEventHandler._on_signal)

    @staticmethod
    def _on_signal(sig, frame):
        """
        PRIVATE
        Raise KeyboardInterrupt on sigint and sigterm.
        Call os._exit if more than 2 signals are received in less than a second.

        :param sig:         The signal received by this script if this class is activated
        :type sig:          int
        :param frame:       Not used
        :type frame:        any
        :return:
        """
        log.info("Signal "+to_str(sig)+" received")
        KillEventHandler._quit_msg.append(datetime.datetime.utcnow())
        if len(KillEventHandler._quit_msg) > KillEventHandler.EVENT_COUNT:
            KillEventHandler._quit_msg = KillEventHandler._quit_msg[-KillEventHandler.EVENT_COUNT:]
        if len(KillEventHandler._quit_msg) >= KillEventHandler.EVENT_COUNT:
            if KillEventHandler._quit_msg[-1] - KillEventHandler._quit_msg[0] < KillEventHandler.INTERVAL:
                log.error(os.linesep + "Hard Exit!!!!" + os.linesep)
                flush_loggers()
                os._exit(1)
                return
        raise KeyboardInterrupt()

    def __init__(self):
        raise RuntimeError("Should not be called: KillEventHandler.__init__")


class Pigz(object):
    _check_result = None

    @staticmethod
    def is_installed():
        if Pigz._check_result is None:
            try:
                which("pigz")
                Pigz._check_result = True
            except StandardError:
                Pigz._check_result = False
        return Pigz._check_result


class TimeReference(object):
    # To be sure we use the same date during the whole process
    _now = datetime.datetime.utcnow()

    @staticmethod
    def get():
        return TimeReference._now


# Business logic classes
# ----------------------------------------------------------------------------


class MemoryStorage(object):
    def __init__(self, freq):
        """

        :param freq:    The rotation frequency of backups stored on this storage
        :type freq:     BackupFrequency
        """
        super(MemoryStorage, self).__init__()
        self._freq = freq

    @property
    def freq(self):
        """ :rtype:     BackupFrequency """
        return self._freq

    def should_save(self):
        return self._freq.should_keep(TimeReference.get().date())

    def save(self, source_file, action_fullname, extension):
        raise NotImplemented(self.__class__.__name__ + "::save")

    def remove(self, archive_name):
        raise NotImplemented(self.__class__.__name__ + "::remove")

    def check_writable(self):
        raise NotImplemented(self.__class__.__name__+"::check_writable")

    def list_archives(self, action_fullname=None):
        raise NotImplementedError("Please override MemoryStorage::list_archives")

    def should_keep(self, archive):
        filename = os.path.basename(archive)
        if len(filename) < 10 or filename[8] != '_' or '_' in filename[0:8]:
            return False
        archive_date, archive_name = filename.split("_", 1)
        if not re.match(r"^[0-9]+$", archive_date):
            return False
        try:
            archive_date = datetime.date(year=int(archive_date[0:4]),
                                         month=int(archive_date[4:6]),
                                         day=int(archive_date[6:8]))
        except ValueError:
            return False
        return self._freq.should_keep(archive_date)

    def _archive_name(self, action_fullname, extension):
        return TimeReference.get().strftime("%Y%m%d")+"_"+action_fullname+"."+extension

    @property
    def small_descr(self):
        return "generic storage"


class LocalFolderStorage(MemoryStorage):
    def __init__(self, freq, folder_name):
        super(LocalFolderStorage, self).__init__(freq)
        self._local_folder = folder_name

    def save(self, source_file, action_fullname, extension):
        dest_file = self._archive_name(action_fullname, extension)
        check_run_cmd("cp", source_file, os.path.join(self._local_folder, dest_file))

    def list_archives(self, action_fullname=None):
        results = []
        for filename in os.listdir(self._local_folder):
            full_path = os.path.join(self._local_folder, filename)
            if not os.path.isfile(full_path):
                continue
            if len(filename) < 10 or filename[8] != '_' or '_' in filename[0:8]:
                continue
            archive_date, archive_name = filename.split("_", 1)
            if not re.match(r"^[0-9]+$", archive_date):
                continue
            try:
                archive_date = datetime.date(year=int(archive_date[0:4]),
                                             month=int(archive_date[4:6]),
                                             day=int(archive_date[6:8]))
            except ValueError:
                continue
            if action_fullname and not archive_name.startswith(action_fullname+"."):
                continue
            results.append(full_path)
        return results

    def check_writable(self):
        return Action.check_folder_writable(self._local_folder)

    def remove(self, archive_name):
        os.remove(archive_name)

    @property
    def small_descr(self):
        return "local folder " + self._local_folder

    def __str__(self):
        details = "folder: " + self._local_folder + os.linesep + to_str(self.freq)
        return "Local folder storage:" + os.linesep + indent(details)


class GlacierStorage(MemoryStorage):
    def __init__(self, freq, vault_name, index_file):
        super(GlacierStorage, self).__init__(freq)
        self._vault_name = vault_name
        self._glacier_list_file = index_file

    def save(self, source_file, action_fullname, extension):
        dest_file = self._archive_name(action_fullname, extension)
        self._send_glacier_file(source_file, dest_file)

    def list_archives(self, action_fullname=None):
        results = []
        for filename in self._list_glacier_memories():
            archive_date, archive_name = filename.split("_", 1)
            if action_fullname and not archive_name.startswith(action_fullname+"."):
                continue
            results.append(filename)
        return results

    def check_writable(self):
        return self._check_glacier_access()

    def remove(self, archive):
        self._delete_glacier_file(archive)

    @property
    def small_descr(self):
        return "aws glacier "+self._vault_name

    def __str__(self):
        details = "vault: " + self._vault_name + os.linesep + "index_file: " + self._glacier_list_file
        details += os.linesep + to_str(self.freq)
        return "Aws glacier storage:" + os.linesep + indent(details)

    @property
    def _section_name(self):
        return "glacier"

    def _check_glacier_access(self):
        cache_value = WriteTestCache.is_glacier_success(self._vault_name)
        if cache_value is None:
            try:
                with temp_filename() as filename:
                    with open(filename, "w") as fh:
                        fh.write("foo bar")
                    self._send_glacier_file(filename, "backup glacier access test")
                    self._delete_glacier_file("backup glacier access test")
                WriteTestCache.set_glacier_success(self._vault_name, True)
            except StandardError as e:
                WriteTestCache.set_glacier_success(self._vault_name, False)
                return ["Unable to write to glacier vault " + self._vault_name + ": " + to_str(e)]
        elif not cache_value:
            return ["Unable to write to glacier vault " + self._vault_name]
        return []

    def _list_glacier_memories(self):
        file_list = configparser.ConfigParser()
        if not os.path.exists(self._glacier_list_file):
            return []
        with open(self._glacier_list_file, "r") as fh:
            file_list.readfp(fh)
        if not file_list.has_section(self._section_name):
            return []
        return file_list.options(self._section_name)

    def _send_glacier_file(self, filename, archive_name=None):
        if archive_name is None:
            archive_name = os.path.basename(filename)
        vault_region, vault_name = self._vault_name.split(":", 2)
        try:
            archive_id = GlacierStorage._send_glacier_file_boto3(vault_region, vault_name, filename, archive_name)
        except ImportError:
            try:
                archive_id = GlacierStorage._send_glacier_file_boto2(vault_region, vault_name, filename, archive_name)
            except ImportError:
                archive_id = GlacierStorage._send_glacier_file_awscli(vault_region, vault_name, filename, archive_name)

        file_list = configparser.ConfigParser()
        if os.path.exists(self._glacier_list_file):
            with open(self._glacier_list_file, "r") as fh:
                file_list.readfp(fh)
        if not file_list.has_section(self._section_name):
            file_list.add_section(self._section_name)
        file_list.set(self._section_name, archive_name, archive_id)
        with open(self._glacier_list_file, "w") as fh:
            file_list.write(fh)

    def _delete_glacier_file(self, archive_name):
        file_list = configparser.ConfigParser()
        if not os.path.exists(self._glacier_list_file):
            raise RuntimeError("Unable to find glacier archive list file "+self._glacier_list_file)
        with open(self._glacier_list_file, "r") as fh:
            file_list.readfp(fh)
        if not file_list.has_section(self._section_name):
            raise RuntimeError("Unable to find glacier archive file " + archive_name)
        if not file_list.has_option(self._section_name, archive_name):
            raise RuntimeError("Unable to find glacier archive file " + archive_name)
        archive_id = file_list.get(self._section_name, archive_name)

        vault_region, vault_name = self._vault_name.split(":", 2)
        try:
            GlacierStorage._delete_glacier_file_boto3(vault_region, vault_name, archive_id)
        except ImportError:
            try:
                GlacierStorage._delete_glacier_file_boto2(vault_region, vault_name, archive_id)
            except ImportError:
                GlacierStorage._delete_glacier_file_awscli(vault_region, vault_name, archive_id)

        file_list = configparser.ConfigParser()
        if not os.path.exists(self._glacier_list_file):
            return
        with open(self._glacier_list_file, "r") as fh:
            file_list.readfp(fh)
        if not file_list.has_section(self._section_name):
            return
        file_list.remove_option(self._section_name, archive_name)
        with open(self._glacier_list_file, "w") as fh:
            file_list.write(fh)

    @staticmethod
    def _send_glacier_file_awscli(vault_region, vault_name, filename, archive_name):
        aws_cli_path = which("aws")
        out = check_run_cmd(aws_cli_path, 'glacier', 'upload-archive', '--account-id', '-', "--body", filename,
                            '--archive-description', archive_name, "--region", vault_region, "--vault-name", vault_name)
        upload_info = json.loads(out)
        if 'archiveId' not in upload_info or not upload_info['archiveId'] or not upload_info['archiveId'].strip():
            raise RuntimeError("Unable to start glacier upload")
        return upload_info['archiveId'].strip()

    @staticmethod
    def _send_glacier_file_boto2(vault_region, vault_name, filename, archive_name):
        import boto, boto.glacier
        conn = boto.glacier.connect_to_region(vault_region)
        vault = conn.get_vault(vault_name)
        return vault.upload_archive(filename, description=archive_name)

    @staticmethod
    def _send_glacier_file_boto3(vault_region, vault_name, filename, archive_name):
        import boto3
        client = boto3.client('glacier', region_name=vault_region)
        with open(filename, 'rb') as f:
            response = client.upload_archive(vaultName=vault_name,
                                             archiveDescription=archive_name,
                                             body=f)
            return response['archiveId']

    @staticmethod
    def _delete_glacier_file_awscli(vault_region, vault_name, archive_id):
        aws_cli_path = which("aws")
        check_run_cmd(aws_cli_path, 'glacier', 'delete-archive', '--account-id', '-', '--archive-id='+archive_id,
                      "--region", vault_region, "--vault-name", vault_name)

    @staticmethod
    def _delete_glacier_file_boto2(vault_region, vault_name, archive_id):
        import boto, boto.glacier
        conn = boto.glacier.connect_to_region(vault_region)
        vault = conn.get_vault(vault_name)
        return vault.delete_archive(archive_id)

    @staticmethod
    def _delete_glacier_file_boto3(vault_region, vault_name, archive_id):
        import boto3
        client = boto3.client('glacier', region_name=vault_region)
        client.delete_archive(vaultName=vault_name, archiveId=archive_id)


class Report(object):
    def __init__(self):
        self._server_issues = {}
        self._server_success = {}
        self._server_warnings = {}

    def add_issue(self, server, error_details):
        if server not in self._server_issues.keys():
            self._server_issues[server] = []
        self._server_issues[server].append(error_details)

    def add_success(self, server, success_details):
        if server not in self._server_success.keys():
            self._server_success[server] = []
        self._server_success[server].append(success_details)

    def add_warning(self, server, warning_details):
        if server not in self._server_warnings.keys():
            self._server_warnings[server] = []
        self._server_warnings[server].append(warning_details)

    @property
    def all_success(self):
        return self._server_success

    @property
    def all_issues(self):
        return self._server_issues

    @property
    def all_warnings(self):
        return self._server_warnings

    @property
    def by_server(self):
        result = dict()
        for server, errors in self._server_issues.items():
            if server not in result.keys():
                result[server] = []
            for error in errors:
                result[server].append({"level": "error", "info": error})
        for server, errors in self._server_warnings.items():
            if server not in result.keys():
                result[server] = []
            for error in errors:
                result[server].append({"level": "warning", "info": error})
        for server, errors in self._server_success.items():
            if server not in result.keys():
                result[server] = []
            for error in errors:
                result[server].append({"level": "server", "info": error})
        return result

    @property
    def issue_count(self):
        count = 0
        for server_issues in self._server_issues.values():
            count += len(server_issues)
        return count

    @property
    def warning_count(self):
        count = 0
        for server_warning in self._server_warnings.values():
            count += len(server_warning)
        return count

    @property
    def is_success(self):
        return not self.is_empty and self.issue_count == 0

    @property
    def is_empty(self):
        for server_issues in self._server_issues.values():
            if len(server_issues) > 0:
                return False
        for server_warnings in self._server_warnings.values():
            if len(server_warnings) > 0:
                return False
        for server_success in self._server_success.values():
            if len(server_success) > 0:
                return False
        return True


class Action(object):
    _SSH_CMD = ["ssh", '-F', '/dev/null', '-o', 'UserKnownHostsFile=/dev/null', '-o', 'StrictHostKeyChecking=no',
                '-o', 'BatchMode=yes', "-o", "LogLevel=ERROR"]

    def __init__(self, server_name, prefix, name, dest_folder, ssh_user, ssh_key):
        super(Action, self).__init__()
        self._server_name = server_name
        self._prefix = prefix
        self._name = name
        self._dest_folder = dest_folder
        self._ssh_user = ssh_user
        self._ssh_key = ssh_key
        self._storage_list = []

    @property
    def storage_list(self):
        """:rtype: list[MemoryStorage]"""
        return self._storage_list

    def add_storage(self, storage):
        self._storage_list.append(storage)

    @property
    def server_name(self):
        return self._server_name

    @property
    def name(self):
        return self._name

    @property
    def prefix(self):
        return self._prefix

    @property
    def full_name(self):
        return self.prefix + "_" + self._name

    @property
    def is_local(self):
        return self.server_name == "local"

    @property
    def small_descr(self):
        if self.is_local:
            return "local " + self.name
        return self.name + " on " + self.server_name

    def check_src_access(self):
        raise NotImplemented(self.__class__.__name__+"::check_src_access")

    def run_backup(self):
        raise NotImplemented(self.__class__.__name__ + "::run_backup")

    def check_dest_access(self):
        detected_errors = []
        detected_errors.extend(Action.check_folder_writable(self._dest_folder))
        for storage in self._storage_list:
            try:
                detected_errors.extend(storage.check_writable())
            except StandardError as e:
                detected_errors.append(to_str(e))
        return detected_errors

    def _get_ssh_args(self, include_remote=True):
        args = copy.copy(Action._SSH_CMD)
        if self._ssh_key:
            args.extend(['-o', 'IdentitiesOnly=yes', '-i', self._ssh_key])
        if include_remote:
            args.append(self._ssh_user + "@" + self._server_name)
        return args

    def _check_ssh_connection(self):
        cmd = self._get_ssh_args()
        cmd.extend(["echo", "ping_test"])
        out = check_run_cmd(*cmd)
        if to_str(out).strip() != "ping_test":
            raise RuntimeError("Bad output:" + to_str(out))

    def __repr__(self):
        return "<"+self.small_descr+">"

    @staticmethod
    def check_folder_writable(folder):
        cache_value = WriteTestCache.is_folder_success(folder)
        if cache_value is None:
            try:
                with temp_filename(dir=folder) as filename:
                    with open(filename, "w") as fh:
                        fh.write("foo bar")
                WriteTestCache.set_folder_success(folder, True)
            except StandardError as e:
                WriteTestCache.set_folder_success(folder, False)
                return ["Unable to write to folder " + folder + ": " + to_str(e)]
        elif not cache_value:
            return ["Unable to write to folder " + folder + " (see previous errors)"]
        return []


class FileAction(Action):
    def __init__(self, server_name, prefix, name, dest_folder, ssh_user, ssh_key, remote_folder, exclusions):
        super(FileAction, self).__init__(server_name, prefix, name, dest_folder, ssh_user, ssh_key)
        self._remote_folder = remote_folder
        self._exclusions = exclusions

    @property
    def small_descr(self):
        return self._remote_folder+" folder on "+self.server_name

    @property
    def remote_folder(self):
        return self._remote_folder

    def check_src_access(self):
        detected_errors = []
        if not self.is_local:
            try:
                self._check_ssh_connection()
            except StandardError as e:
                return ["Unable to connect to server " + self._server_name + ": " + to_str(e)]
        detected_errors.extend(self._check_folder_readable(self._remote_folder, self._exclusions))
        return detected_errors

    def run_backup(self):
        log.info(self.small_descr+": Starting backup...")

        log.info(self.small_descr + ": " + indent() + "fetching data...")
        cmd = ["rsync", "--delete", "-a"]
        if not self.is_local:
            cmd.extend(["-e", " ".join(map(shell_quote, self._get_ssh_args(False)))])
        for exclusion in self._exclusions:
            cmd += ["--exclude="+exclusion[len(self.remote_folder)+1:]]
        src = self._remote_folder if self.is_local else self._ssh_user+"@"+self._server_name+":"+self._remote_folder
        cmd.extend([src, os.path.join(self._dest_folder, self.full_name)])
        check_run_cmd(cmd)
        check_run_cmd("touch", os.path.join(self._dest_folder, self.full_name, ".backup_date"))
        log.info(self.small_descr+": " + indent() + "data fetch")

        log.info(self.small_descr + ": " + indent() + "compressing data...")
        with temp_filename(suffix=".tgz") as archive_filename:
            cmd = ["nice", "-2", "tar", "-c"]
            if Pigz.is_installed():
                cmd.append("--use-compress-program=pigz")
            cmd.extend(["-C", self._dest_folder, '-f', archive_filename, self.full_name])
            check_run_cmd(cmd)
            log.info(self.small_descr + ": " + indent() + "data compressed")

            log.info(self.small_descr + ": " + indent() + "saving data...")
            for storage in self.storage_list:
                if not storage.should_save():
                    continue
                log.info(self.small_descr + ": " + indent() + indent() + "saving on " + storage.small_descr + "...")
                storage.save(archive_filename, self.full_name, "tgz")
                log.info(self.small_descr + ": " + indent() + indent() + "saved on " + storage.small_descr)
            log.info(self.small_descr + ": " + indent() + "data saved")
        log.info(self.small_descr + ": Backup completed")

    def __str__(self):
        details = "remote file: " + self._remote_folder
        details += os.linesep + "exclusions: " + ", ".join([shell_quote(f) for f in self._exclusions])
        details += os.linesep + "ssh user: " + (self._ssh_user if self._ssh_user else "Default")
        details += os.linesep + "ssh key: " + (self._ssh_key if self._ssh_key else "Default")
        details += os.linesep + "local destination: " + self._dest_folder
        details += os.linesep + "storage_list: "
        if self._storage_list:
            for storage in self._storage_list:
                details += os.linesep + indent(to_str(storage))
        else:
            details += "none"
        return "File action " + self.full_name + " on " + self.server_name + ": " + os.linesep + indent(details)

    def _check_folder_readable(self, folder, exclusions):
        server_description = "local machine" if self.is_local else "server " + self._server_name
        cmd = [] if self.is_local else self._get_ssh_args()
        cmd.extend(["find", folder])
        for to_exclude in exclusions:
            if self.is_local:
                cmd.extend(["-not", "(", "-path", to_exclude, "-prune", ")"])
            else:
                cmd.extend(["-not", "\\(", "-path", to_exclude, "-prune", "\\)"])
        cmd.extend(["-not", "-readable", "-not", "-type", "l"])
        try:
            out = check_run_cmd(*cmd).strip()
            if out:
                return ["Unable to read some files in folder " + folder + " on " + server_description +
                        ": " + os.linesep + indent("Command: " + " ".join([shell_quote(arg) for arg in cmd])) +
                        os.linesep + indent("Files:" + os.linesep + indent(to_str(out)))]
        except StandardError as e:
            raise CheckError("Unable to read folder " + folder + " on " + server_description + ": " + to_str(e))
        return []


class DbAction(Action):
    def __init__(self, server_name, prefix, name, dest_folder, ssh_user, ssh_key, db_user, db_name, db_port):
        super(DbAction, self).__init__(server_name, prefix, name, dest_folder, ssh_user, ssh_key)
        self._db_user = db_user
        self._db_name = db_name
        self._db_port = db_port

    def run_backup(self):
        log.info(self.small_descr+": Starting backup...")

        log.info(self.small_descr + ": " + indent() + "fetching data...")
        ext = self._get_extension()+".gz"
        local_archive = os.path.join(self._dest_folder, self.full_name+"."+ext)
        self._save_database(local_archive)
        log.info(self.small_descr + ": " + indent() + "data fetch")
        log.info(self.small_descr + ": " + indent() + "saving data...")
        for storage in self.storage_list:
            if not storage.should_save():
                continue
            log.info(self.small_descr + ": " + indent() + indent() + "saving on " + storage.small_descr + "...")
            storage.save(local_archive, self.full_name, ext)
            log.info(self.small_descr + ": " + indent() + indent() + "saved on " + storage.small_descr)
        log.info(self.small_descr + ": " + indent() + "data saved")
        log.info(self.small_descr + ": Backup completed")

    @property
    def database(self):
        return self._db_name

    @property
    def db_type(self):
        raise NotImplementedError("You should not instanciate a DbAtion directly")

    @property
    def small_descr(self):
        return self.database + " " + self.db_type + " database on " + self.server_name

    def _get_extension(self):
        return "sql"

    def _save_database(self, dest_file):
        raise NotImplemented(self.__class__.__name__+"::_save_database")


class MySqlAction(DbAction):
    def __init__(self, server_name, prefix, name, dest_folder, ssh_user, ssh_key, db_user, db_name, db_port):
        super(MySqlAction, self).__init__(server_name, prefix, name, dest_folder, ssh_user, ssh_key, db_user, db_name,
                                          db_port)

    @property
    def db_type(self):
        return "mysql"

    def _save_database(self, dest_file):
        dump_cmd = ['mysqldump', '-u', self._db_user, "-h", "localhost", "--port="+to_str(self._db_port),
               '--databases', self._db_name]

        if self.is_local:
            cmd_str = " ".join(map(shell_quote, dump_cmd)) + " | gzip > "+shell_quote(dest_file)
        else:
            cmd = self._get_ssh_args()
            cmd.append(" ".join(map(shell_quote, dump_cmd)) + " | gzip")
            cmd_str = " ".join(map(shell_quote, cmd)) + " > "+shell_quote(dest_file)
        pipes = subprocess.Popen(cmd_str, stderr=subprocess.PIPE, shell=True)
        std_out, std_err = pipes.communicate()
        exit_code = pipes.returncode
        if exit_code != 0:
            error = "Command failed with exit code " + to_str(exit_code) + os.linesep
            error += "  Command: " + cmd_str
            err = to_str(std_err).strip()
            if err:
                error += os.linesep + "  Error output:" + os.linesep + indent(err, indent_str="    ")
            raise RuntimeError(error)

    def check_src_access(self):
        server_description = "local machine" if self.is_local else "server " + self._server_name
        cmd = [] if self.is_local else self._get_ssh_args()
        cmd.extend(["mysql", '--batch', '-D', self._db_name, '-b', "-s", "-N", "-P", to_str(self._db_port),
                    '-u', self._db_user, "-e", shell_quote("SHOW TABLES")])
        try:
            table_list = to_str(check_run_cmd(*cmd)).splitlines()
        except StandardError as e:
            return ["Unable to connect to mysql database " + self._db_name + " on " + server_description + ": " +
                    to_str(e)]
        error_list = []
        for table in table_list:
            try:
                cmd = [] if self.is_local else self._get_ssh_args()
                cmd.extend(["mysql", '--batch', '-D', self._db_name, '-b', "-s", "-N", "-P", to_str(self._db_port),
                            '-u', self._db_user, "-e", shell_quote("SELECT * FROM `" + table + "` LIMIT 1")])
                check_run_cmd(*cmd)
            except StandardError as e:
                error_list.append("Unable to read mysql table " + self._db_name + "." + table + " on " +
                                  server_description + ": " + to_str(e))
        return error_list

    def __str__(self):
        details = "database name: " + self._db_name
        details += os.linesep + "database port: " + to_str(self._db_port)
        details += os.linesep + "database user: " + self._db_user
        details += os.linesep + "ssh user: " + (self._ssh_user if self._ssh_user else "Default")
        details += os.linesep + "ssh key: " + (self._ssh_key if self._ssh_key else "Default")
        details += os.linesep + "local destination: " + self._dest_folder
        details += os.linesep + "storage_list: "
        if self._storage_list:
            for storage in self._storage_list:
                details += os.linesep + indent(to_str(storage))
        else:
            details += "none"
        return "MySql action " + self.full_name + " on " + self.server_name + ": " + os.linesep + indent(details)


class PostgresAction(DbAction):
    def __init__(self, server_name, prefix, name, dest_folder, ssh_user, ssh_key, db_user, db_name, db_port):
        super(PostgresAction, self).__init__(server_name, prefix, name, dest_folder, ssh_user, ssh_key,
                                             db_user, db_name, db_port)

    @property
    def db_type(self):
        return "postgres"

    def check_src_access(self):
        server_description = "local machine" if self.is_local else "server " + self._server_name
        cmd = [] if self.is_local else self._get_ssh_args()
        if "#" in self._db_name:
            db_name, db_schema = self._db_name.split("#", 2)
        else:
            db_name, db_schema = (self._db_name, None)
        query = "SELECT schemaname || '#!#' || tablename FROM pg_catalog.pg_tables "
        if db_schema is None:
            query += "WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';"
        else:
            query += "WHERE schemaname = '"+db_schema+"';"
        cmd.extend(["psql", '-p', to_str(self._db_port), "-U", self._db_user, "-d", db_name, "-t", '-c',
                    shell_quote(query)])
        try:
            table_list = [table.strip() for table in to_str(check_run_cmd(*cmd)).strip().splitlines()]
        except StandardError as e:
            return ["Unable to connect to postgres database " + self._db_name + " on " + server_description + ": " +
                    to_str(e)]
        error_list = []
        for table in table_list:
            try:
                cmd = [] if self.is_local else self._get_ssh_args()
                quoted_table = ".".join(['"'+el+'"' for el in table.split("#!#", 2)])
                cmd.extend(["psql", '-p', to_str(self._db_port), "-U", self._db_user, "-d", db_name, "-t", '-c',
                            shell_quote("SELECT * FROM " + quoted_table + " LIMIT 1")])
                check_run_cmd(*cmd)
            except StandardError as e:
                error_list.append("Unable to read mysql table " + self._db_name + "." + table + " on " +
                                  server_description + ": " + to_str(e))
        return error_list

    def _save_database(self, dest_file):
        dump_cmd = ['pg_dump', "-h", "localhost", "-p", to_str(self._db_port), "-d", self._db_name]

        if self.is_local:
            cmd_str = " ".join(map(shell_quote, dump_cmd)) + " | gzip > "+shell_quote(dest_file)
        else:
            cmd = self._get_ssh_args()
            cmd.append(" ".join(map(shell_quote, dump_cmd)) + " | gzip")
            cmd_str = " ".join(map(shell_quote, cmd)) + " > "+shell_quote(dest_file)
        pipes = subprocess.Popen(cmd_str, stderr=subprocess.PIPE, shell=True)
        std_out, std_err = pipes.communicate()
        exit_code = pipes.returncode
        if exit_code != 0:
            error = "Command failed with exit code " + to_str(exit_code) + os.linesep
            error += "  Command: " + cmd_str
            err = to_str(std_err).strip()
            if err:
                error += os.linesep + "  Error output:" + os.linesep + indent(err, indent_str="    ")
            raise RuntimeError(error)

    def __str__(self):
        details = "database name: " + self._db_name
        details += os.linesep + "database port: " + to_str(self._db_port)
        details += os.linesep + "database user: " + self._db_user
        details += os.linesep + "ssh user: " + (self._ssh_user if self._ssh_user else "Default")
        details += os.linesep + "ssh key: " + (self._ssh_key if self._ssh_key else "Default")
        details += os.linesep + "local destination: " + self._dest_folder
        details += os.linesep + "storage_list: "
        if self._storage_list:
            for storage in self._storage_list:
                details += os.linesep + indent(to_str(storage))
        else:
            details += "none"
        return "Postgres action " + self.full_name + " on " + self.server_name + ": " + os.linesep + indent(details)


class MongoDbAction(DbAction):
    def __init__(self, server_name, prefix, name, dest_folder, ssh_user, ssh_key, db_user, db_name, db_port):
        super(MongoDbAction, self).__init__(server_name, prefix, name, dest_folder, ssh_user, ssh_key, db_user, db_name,
                                            db_port)

    @property
    def db_type(self):
        return "mongo"

    def check_src_access(self):
        server_description = "local machine" if self.is_local else "server " + self._server_name
        cmd = [] if self.is_local else self._get_ssh_args()
        cmd.extend(["mongo", '--port', to_str(self._db_port), self._db_name, '--eval',
                    shell_quote("printjson(db.getCollectionNames())")])
        try:
            check_run_cmd(*cmd)
        except StandardError as e:
            return ["Unable to connect to mongo database " + self._db_name + " on " + server_description + ": " +
                    to_str(e)]
        return []

    def _save_database(self, dest_file):
        dump_cmd = ['mongodump', "--archive", "--gzip", "--host", "localhost", "--port="+to_str(self._db_port),
                    "--db", self._db_name]

        if self.is_local:
            cmd_str = " ".join(map(shell_quote, dump_cmd)) + " > "+shell_quote(dest_file)
        else:
            cmd = self._get_ssh_args()
            cmd.append(" ".join(map(shell_quote, dump_cmd)))
            cmd_str = " ".join(map(shell_quote, cmd)) + " > "+shell_quote(dest_file)
        pipes = subprocess.Popen(cmd_str, stderr=subprocess.PIPE, shell=True)
        std_out, std_err = pipes.communicate()
        exit_code = pipes.returncode
        if exit_code != 0:
            error = "Command failed with exit code " + to_str(exit_code) + os.linesep
            error += "  Command: " + cmd_str
            err = to_str(std_err).strip()
            if err:
                error += os.linesep + "  Error output:" + os.linesep + indent(err, indent_str="    ")
            raise RuntimeError(error)

    def __str__(self):
        details = "database name: " + self._db_name
        details += os.linesep + "database port: " + to_str(self._db_port)
        details += os.linesep + "database user: " + self._db_user
        details += os.linesep + "ssh user: " + (self._ssh_user if self._ssh_user else "Default")
        details += os.linesep + "ssh key: " + (self._ssh_key if self._ssh_key else "Default")
        details += os.linesep + "local destination: " + self._dest_folder
        details += os.linesep + "storage_list: "
        if self._storage_list:
            for storage in self._storage_list:
                details += os.linesep + indent(to_str(storage))
        else:
            details += "none"
        return "MongoDb action " + self.full_name + " on " + self.server_name + ": " + os.linesep + indent(details)

    def _get_extension(self):
        return "mongo"


class WriteTestCache(object):
    _folder_cache = {}
    _glacier_vault_cache = {}

    @staticmethod
    def is_folder_success(folder):
        """
        :return: True if well tested, False if test failed, and None if not tested
        """
        if folder in WriteTestCache._folder_cache.keys():
            return WriteTestCache._folder_cache[folder]
        return None

    @staticmethod
    def set_folder_success(folder, value):
        WriteTestCache._folder_cache[folder] = value

    @staticmethod
    def is_glacier_success(vault):
        """
        :return: True if well tested, False if test failed, and None if not tested
        """
        if vault in WriteTestCache._glacier_vault_cache.keys():
            return WriteTestCache._glacier_vault_cache[vault]
        return None

    @staticmethod
    def set_glacier_success(vault, value):
        WriteTestCache._glacier_vault_cache[vault] = value


class ReportTarget(object):
    def __init__(self):
        super(ReportTarget, self).__init__()

    def report(self, report):
        if report.is_empty:
            return
        raise NotImplementedError("Please override ReportTarget::report")

    def _render_template(self, template, **kwargs):
        import jinja2
        template = jinja2.Template(template)
        return template.render(**kwargs)

    def _get_default_content(self, report):
        """


        :param report:
        :type report:       Report
        :return:
        :rtype:             str
        """
        content = "Backup report: "
        if report.issue_count > 0:
            content += to_str(report.issue_count)+" errors"
            if report.warning_count > 0:
                content += " and "
        if report.warning_count > 0:
            content += to_str(report.warning_count)+" warnings"
        elif report.is_success:
            content += "success"
        content += "\n"

        for server, info_list in report.by_server.items():
            content += indent(server + ": ")+"\n"
            for info in info_list:
                content += indent(indent(info["level"]+": "+info["info"]))+"\n"
        return content

    def _format_report(self, report):
        if report.is_success:
            subject = "Backup: success"
        else:
            subject = "Backup: " + to_str(report.issue_count) + " errors"
        return subject, self._get_default_content(report)

    def __repr__(self):
        return to_str(self)


class FileReportTarget(ReportTarget):
    def __init__(self, file_names, template=None):
        super(FileReportTarget, self).__init__()
        self._file_names = file_names
        self._template = template

    def report(self, report):
        if report.is_empty:
            return

        _, content = self._format_report(report)
        for file in self._file_names:
            with open(file, "a+") as fh:
                fh.write(content+os.linesep+os.linesep)

    def _format_report(self, report):
        if self._template is not None:
            content = self._render_template(self._template, report=report)
        else:
            content = self._get_default_content(report)
        return None, content

    def __str__(self):
        details = "files: " + ", ".join(self._file_names)
        if self._template:
            details += os.linesep + "template: yes" + os.linesep + indent(self._template)
        else:
            details += os.linesep + "template: no"
        return "File report target: "+os.linesep + indent(details)


class SyslogReportTarget(ReportTarget):
    def __init__(self, template=None):
        super(SyslogReportTarget, self).__init__()
        self._template = template

    def report(self, report):
        if report.is_empty:
            return

        _, content = self._format_report(report)
        syslog.openlog("backuper", logoption=syslog.LOG_PID)
        try:
            if report.issue_count:
                syslog.syslog(syslog.LOG_ERR, content)
            elif report.warning_count:
                syslog.syslog(syslog.LOG_WARNING, content)
            else:
                syslog.syslog(syslog.LOG_INFO, content)
        finally:
            syslog.closelog()

    def _format_report(self, report):
        if self._template is not None:
            content = self._render_template(self._template, report=report)
        else:
            content = self._get_default_content(report)
        return None, content

    def __str__(self):
        if self._template:
            details = "template: yes" + os.linesep + indent(self._template)
        else:
            details = "template: no"
        return "Syslog report target: "+os.linesep + indent(details)


class WebhookReportTarget(ReportTarget):
    def __init__(self, url_list, params=None, post_method=True, use_json=True, mapping=None):
        super(WebhookReportTarget, self).__init__()
        self._webhook_urls = url_list
        self._webhook_params = params
        self._mapping = mapping
        self._post_method = post_method
        self._use_json = use_json

    def report(self, report):
        if report.is_empty:
            return

        params = self._format_report_params(report, self._webhook_params)

        for url in self._webhook_urls:
            import requests

            if self._post_method:
                if self._use_json:
                    r = requests.post(url, json=params)
                else:
                    r = requests.post(url, data=params)
            else:
                r = requests.get(url, params)

            if r.status_code != 200:
                raise RuntimeError("Unable to send rocket-chat message to " + url + ": " + os.linesep +
                                   indent("HTTP Error: " + to_str(r.status_code) + os.linesep + "Content:" +
                                          os.linesep + indent(r.content)))

    def _format_report_params(self, report, params):
        if params is None:
            params = {}
        result = self._mapping(report, params)
        return result

    def __str__(self):
        if len(self._webhook_urls) > 1:
            details = "urls: "
            for url in self._webhook_urls:
                details += os.linesep + indent(url)
        else:
            details = "url: "+self._webhook_urls[0]
        details += os.linesep + "method: " + ("POST" if self._post_method else "GET")
        details += os.linesep + "json request: " + ("yes" if self._use_json else "no")
        if self._webhook_params:
            details += os.linesep + "params: " + repr(self._webhook_params)
        else:
            details += os.linesep + "params: none"
        if self._mapping:
            details += os.linesep + "mapping: " + to_str(self._mapping)
        else:
            details += os.linesep + "mapping: none"
        return "Webhook report target: "+os.linesep + indent(details)


class EmailReportTarget(ReportTarget):
    def __init__(self, email_dest, email_prefix, email_sender, email_user, email_pwd, email_server, email_port,
                 template=None):
        super(EmailReportTarget, self).__init__()
        self._email_dest = email_dest
        self._email_prefix = email_prefix
        self._email_sender = email_sender
        self._email_user = email_user
        self._email_pwd = email_pwd
        self._email_server = email_server
        self._email_port = email_port
        self._template = template

    def report(self, report):
        if report.is_empty:
            return
        subject, content = self._format_report(report)
        msg = email.mime.text.MIMEText(content)
        msg['Subject'] = (self._email_prefix if self._email_prefix else "") + subject
        if self._email_sender:
            msg['From'] = self._email_sender
        elif self._email_user and "@" in self._email_user:
            msg['From'] = self._email_user
        else:
            msg['From'] = self._email_dest[0]
        msg['To'] = ", ".join(self._email_dest)

        if self._email_user:
            smtp_user = self._email_user
        elif self._email_sender:
            smtp_user = self._email_sender
        else:
            smtp_user = None
        sender_server = email.utils.parseaddr(msg['From'])[1]
        if not self._email_port or self._email_port == 25:
            smtp_conn = smtplib.SMTP(self._email_server, 25, sender_server)
        else:
            smtp_conn = smtplib.SMTP_SSL(self._email_server, self._email_port, sender_server)
        try:
            if self._email_pwd:
                smtp_conn.login(smtp_user, self._email_pwd)
            smtp_conn.sendmail(msg['From'], self._email_dest, msg.as_string())
        finally:
            smtp_conn.quit()

    def _format_report(self, report):
        if report.is_success:
            subject = "Backup: success"
        else:
            subject = "Backup: " + to_str(report.issue_count) + " errors"
        if self._template:
            content = self._render_template(self._template, report=report)
        else:
            content = self._get_default_content(report)
        return subject, content

    def __str__(self):
        details = "to: "
        if len(self._email_dest) > 1:
            for dest in self._email_dest:
                details += os.linesep + indent(dest)
        else:
            details += self._email_dest[0]
        details += os.linesep + "prefix: " + (self._email_prefix if self._email_prefix else "none")
        details += os.linesep + "sender: " + self._email_sender
        details += os.linesep + "smtp user: " + self._email_user
        details += os.linesep + "smtp password: " + self._email_pwd
        details += os.linesep + "smtp server: " + self._email_server
        details += os.linesep + "smtp port: " + to_str(self._email_port)
        if self._template:
            details += os.linesep + "template: yes" + os.linesep + indent(self._template)
        else:
            details += os.linesep + "template: no"
        return "Email report target: "+os.linesep + indent(details)


class BackupFrequency(object):
    ALL_STR = "all"
    ALL_VALUE = -1
    NO_STR = "no"
    NO_VALUE = 0

    FREQ_DAY = "day"
    FREQ_WEEK = "week"
    FREQ_MONTH = "month"
    FREQ_YEAR = "year"

    def __init__(self, day, week, month, year):
        self._day = day
        self._week = week
        self._month = month
        self._year = year

    def is_empty(self):
        for freq in [self._day, self._week, self._month, self._year]:
            if freq != BackupFrequency.NO_VALUE:
                return False
        return True

    def need_deletion(self):
        for freq in [self._day, self._week, self._month, self._year]:
            if freq not in (BackupFrequency.ALL_VALUE, BackupFrequency.NO_VALUE):
                return True
        return False

    def should_keep(self, date):
        """

        :param date:
        :type date:         datetime.date
        :return:
        :rtype:             bool
        """
        if self._day == BackupFrequency.ALL_VALUE:
            return True
        if self._day != BackupFrequency.NO_VALUE:
            elapsed_days = (TimeReference.get().date() - date).days + 1
            if elapsed_days <= self._day:
                return True
        if date.isoweekday() == 1:
            if self._week == BackupFrequency.ALL_VALUE:
                return True
            if self._week != BackupFrequency.NO_VALUE:
                last_allowed_date = TimeReference.get().date()
                delta = datetime.timedelta(days=last_allowed_date.isoweekday() + (7 * self._week) - 8)
                last_allowed_date = last_allowed_date - delta
                if date >= last_allowed_date:
                    return True
        if date.day == 1:
            if self._month == BackupFrequency.ALL_VALUE:
                return True
            if self._month != BackupFrequency.NO_VALUE:
                now = TimeReference.get().date()
                last_allowed_date = datetime.date(
                    year=now.year + int(math.floor((now.month - self._month)/12)),
                    month=((now.month - self._month) % 12)+1,
                    day=1
                )
                if date >= last_allowed_date:
                    return True
            if date.month == 1:
                if self._year == BackupFrequency.ALL_VALUE:
                    return True
                if self._year != BackupFrequency.NO_VALUE:
                    now = TimeReference.get().date()
                    last_allowed_date = datetime.date(
                        year=now.year - self._year + 1,
                        month=1,
                        day=1
                    )
                    if date >= last_allowed_date:
                        return True
        return False

    @staticmethod
    def freq_to_str(value):
        if value == BackupFrequency.ALL_VALUE:
            return BackupFrequency.ALL_STR
        if value == BackupFrequency.NO_VALUE:
            return BackupFrequency.NO_STR
        return to_str(value)

    def __str__(self):
        details = "day: " + BackupFrequency.freq_to_str(self._day)
        details += os.linesep + "week: " + BackupFrequency.freq_to_str(self._week)
        details += os.linesep + "month: " + BackupFrequency.freq_to_str(self._month)
        details += os.linesep + "year: " + BackupFrequency.freq_to_str(self._year)
        return "frequency: " + os.linesep + indent(details)


class MappingFunction(object):
    def __init__(self, filename, function_name=None):
        self._filename = filename
        self._function_name = function_name
        old_path = copy.deepcopy(sys.path)
        try:
            module_name = os.path.splitext(os.path.basename(filename))[0]
            sys.path.insert(0, os.path.dirname(filename))
            mapping_module = importlib.import_module(module_name, package=None)
            if function_name:
                self._function = getattr(mapping_module, function_name)
            else:
                self._function = getattr(mapping_module, "__main__", None)
        finally:
            sys.path = old_path

    def __str__(self):
        if self._function_name:
            return self._function_name + " function in " + self._filename
        else:
            return "file " + self._filename

    def __call__(self, *args, **kwargs):
        return self._function(*args, **kwargs)


class BackupConfig(object):
    def __init__(self, config_file):
        """
        :param config_file: 	The config file to load
        :type config_file:		str
        """
        super(BackupConfig, self).__init__()
        self._actions, self._report_list = self._load_conf(config_file)

    @property
    def server_list(self):
        return list(set([action.server_name for action in self._actions]))

    @property
    def report_list(self):
        return self._report_list

    def get_actions(self):
        return self._actions

    def show(self, targets):
        output = "Actions: "+os.linesep
        output += (os.linesep+os.linesep).join([indent(to_str(a)) for a in targets])
        output += os.linesep + os.linesep + os.linesep
        output += "Reports: "+os.linesep
        output += (os.linesep + os.linesep).join([indent(to_str(r)) for r in self._report_list])
        log.info(output)

    @staticmethod
    def _get_loader():
        try:
            from ruamel.yaml import YAML
            return YAML(typ='safe')
        except ImportError:
            try:
                import yaml
                return yaml
            except ImportError:
                import json
                return json

    @staticmethod
    def _load_conf(config_file):
        """
        Load the configuration file

        :param config_file:		The config file to load
        :type config_file:		str
        """
        actions = []
        report_targets = []
        data = None

        loader = BackupConfig._get_loader()
        with open(config_file, "r") as fh:
            try:
                data = loader.load(fh)
            except StandardError as e:
                raise ConfigError("Invalid format for config file " + to_str(e))

        # First parsing, interpret the 'include' directives
        data = BackupConfig._parse_includes(data, config_file, loader)

        # Secondary parsing, just organise data
        common_info = {}
        server_info_dict = {}
        report_info_list = []
        common_keys = extract_keys(data, "ssh_user", "ssh_key", "dest_folder", "db_user")
        for key, info in data.items():
            key = key.lower().strip()
            if not key:
                continue
            if is_primitive(info):
                raise ConfigError(key + " section can't be a single value")
            if key in ("common", "global"):
                common_info = deep_merge(common_info, info)
            elif key == "report":
                report_info_list = deep_merge(report_info_list, to_list(info))
            else:
                server_info_dict = deep_merge(server_info_dict, {key: to_list(info)})
        common_info = deep_merge(common_keys, common_info)

        # Third parsing: creating structures for data
        for server_name, info_list in server_info_dict.items():
            for info in info_list:
                # Merge common information
                server_info = copy.deepcopy(common_info)
                server_info.update(info)

                # Generate storage objects
                storage_list = []
                store_info = extract_keys(server_info, "local_history", "local_history_folder", "local_history_memory")
                storage_list.extend(BackupConfig._parse_local_storage_list(store_info, server_name))
                store_info = extract_keys(server_info, "aws_glacier", "aws_glacier_memory", "aws_glacier_vault",
                                          "aws-glacier_index_file")
                storage_list.extend(BackupConfig._parse_glacier_storage_list(store_info, server_name))

                # Extract special information
                files_info = extract_keys(server_info, "files")
                databases_info = extract_keys(server_info, "databases", "db_user")

                if "files" in files_info:
                    file_excludes = []
                    files_info = files_info["files"]
                    if is_string(files_info):
                        files_info = [files_info]

                    if is_array(files_info):
                        files_info = {BackupConfig._filename_to_prefix(files_info): f for f in files_info}

                    if is_dict(files_info):
                        if "exclude" in files_info.keys():
                            file_excludes = files_info['exclude']
                            del files_info['exclude']
                    else:
                        raise ConfigError("Invalid 'files' section for server "+server_name+": "+repr(files_info))

                    # Create file backup structure
                    for name, file_info in files_info.items():
                        action = BackupConfig._parse_file_action_conf(server_name, name, server_info, file_info,
                                                                      file_excludes)
                        for storage in storage_list:
                            action.add_storage(storage)
                        actions.append(action)

                if "databases" in databases_info:
                    if "db_user" in databases_info.keys():
                        db_user = databases_info['db_user']
                    elif "db_user" in common_info.keys():
                        db_user = common_info['db_user']
                    else:
                        db_user = None

                    databases_info = databases_info["databases"]
                    if is_string(databases_info):
                        databases_info = [databases_info]

                    if is_array(databases_info):
                        databases_info = {BackupConfig._db_to_prefix(databases_info): f for f in databases_info}

                    if not is_dict(files_info):
                        raise ConfigError("Invalid 'databases' section for server "+server_name)

                    for name, db_info in databases_info.items():
                        action = BackupConfig._parse_db_action_conf(server_name, name, server_info, db_info, db_user)
                        for storage in storage_list:
                            action.add_storage(storage)
                        actions.append(action)

        # check if two actions does'nt have the same full_name
        for action in actions:
            for other_action in actions:
                if id(action) == id(other_action):
                    continue
                if action.full_name == other_action.full_name:
                    raise ConfigError("Two actions have the same final archive name: " + os.linesep +
                                      indent(to_str(action)) + os.linesep +
                                      "is incompatible with:" + os.linesep +
                                      indent(to_str(other_action)))

        # Fourth parsing: creating structures for reports
        for report_info in report_info_list:
            if report_info == "syslog":
                report_targets.append(BackupConfig._parse_syslog_report(config_file, {}))
            elif is_dict(report_info):
                if len(report_info.keys()) != 1:
                    raise ConfigError("Invalid report configuration: " + repr(report_info))
                key, data = first(report_info)
                key = to_str(key).strip().lower()
                data = to_list(data)
                if key == "email":
                    for report_data in data:
                        report_targets.append(BackupConfig._parse_email_report(config_file, report_data))
                elif key == "file":
                    for report_data in data:
                        report_targets.append(BackupConfig._parse_file_report(config_file, report_data))
                elif key == "webhook":
                    for report_data in data:
                        report_targets.append(BackupConfig._parse_webhook_report(config_file, report_data))
                elif key == "syslog":
                    for report_data in data:
                        report_targets.append(BackupConfig._parse_syslog_report(config_file, report_data))
                else:
                    raise ConfigError("Unknown report type " + key)
            else:
                raise ConfigError("Invalid report configuration: "+repr(report_info))

        return actions, report_targets

    @staticmethod
    def _parse_includes(data, config_file, loader):
        if is_array(data):
            result = []
            for element in data:
                result = deep_merge(result, BackupConfig._parse_includes(element, config_file, loader))
            return result
        elif is_dict(data):
            if data.keys() == ["include"]:
                path = os.path.abspath(os.path.dirname(config_file))
                return BackupConfig._include(data["include"], path, loader)
            result = {}
            for key, val in data.items():
                if key == "include":
                    continue
                result[key] = BackupConfig._parse_includes(val, config_file, loader)
            if "include" in data.keys():
                path = os.path.abspath(os.path.dirname(config_file))
                result = deep_merge(result, BackupConfig._include(data["include"], path, loader))
            return result
        elif is_string(data):
            m = re.match(r"^include *(.*)$", data)
            if m:
                path = os.path.abspath(os.path.dirname(config_file))
                return BackupConfig._include(m.group(0), path, loader)
            return data
        else:
            return data

    @staticmethod
    def _include(file_pattern, path, loader):
        results = None
        with using_cwd(path):
            for filename in glob.glob(file_pattern):
                full_filename = os.path.abspath(filename)
                with open(filename, "r") as fh:
                    try:
                        result = loader.load(fh)
                    except Exception as e:
                        raise ConfigError("Invalid format for config file " + full_filename + ": " +
                                          os.linesep + indent(to_str(e)))
                after_include = BackupConfig._parse_includes(result, full_filename, loader)
                results = deep_merge(results, after_include)
        return results

    @staticmethod
    def _parse_local_storage_list(info, server_name):
        if not info:
            return []
        if "local_history" in info.keys():
            sub_values = info["local_history"]
            del info["local_history"]
            if not is_dict(sub_values):
                raise ConfigError("Invalid 'local_history' parameter for server " + server_name)
            for key, val in sub_values.items():
                key = to_str(key).lower().strip()
                if key not in ("folder", "memory", "local_history_folder", "local_history_memory"):
                    raise ConfigError("Unknown key local_history." + key + " for server " + server_name)
                new_key = key if key.startswith("local_history_") else "local_history_"+key
                info[new_key] = val

        if "local_history_folder" not in info.keys():
            raise ConfigError("Missing local history folder parameter for server " + server_name)
        if "local_history_memory" not in info.keys():
            raise ConfigError("Missing local history memory parameter for server " + server_name)

        folder = info["local_history_folder"]
        if not is_string(folder):
            raise ConfigError("invalid 'local_history_folder' parameter for server " + server_name)
        if not os.path.isabs(folder):
            raise ConfigError("local_history_folder for server " + server_name + " should be an absolute path")
        freq = BackupConfig._parse_freq(info["local_history_memory"])
        return [LocalFolderStorage(freq, folder)]

    @staticmethod
    def _parse_glacier_storage_list(info, server_name):
        if not info:
            return []
        if "aws_glacier" in info.keys():
            sub_values = info["aws_glacier"]
            del info["aws_glacier"]
            if not is_dict(sub_values):
                raise ConfigError("Invalid 'aws_glacier' parameter for server " + server_name)
            for key, val in sub_values.items():
                key = to_str(key).lower().strip()
                if key not in ("vault", "memory", "index_file", "glacier_vault", "glacier_memory",
                               "glacier_index_file"):
                    raise ConfigError("Unknown key glacier." + key + " for server " + server_name)
                new_key = key if key.startswith("aws_glacier_") else "aws_glacier_" + key
                info[new_key] = val

        if "aws_glacier_vault" not in info.keys():
            raise ConfigError("Missing aws glacier vault parameter for server " + server_name)
        if "aws_glacier_memory" not in info.keys():
            raise ConfigError("Missing aws glacier memory parameter for server " + server_name)

        vault = info["aws_glacier_vault"]
        if not is_string(vault) or len(vault.split(":")) != 2:
            raise ConfigError("invalid glacier vault parameter for server " + server_name)
        freq = BackupConfig._parse_freq(info["aws_glacier_memory"])
        index_file = os.path.expanduser("~/.glacier_index")
        if "aws_glacier_index_file" in info.keys():
            index_file = os.path.realpath(os.path.abspath(os.path.expanduser(info["aws_glacier_index_file"])))
        return [GlacierStorage(freq, vault, index_file)]

    @staticmethod
    def _parse_action_common(params, server_name):
        """
        Parse the common options of an action and fill the action object with the data

        :param params: 			The parameters to read and check
        :type params:			dict[str:any]
        :param server_name: 	The name of the server (or local) we are reading the params
        :type server_name:		str
        :return:                A list variables fetched from the params, namely: prefix, dest_folder, ssh_user,
                                ssh_key
        :rtype:                 tuple[str, str|None, str|None, str|None]
        """
        prefix = None
        dest_folder = None
        ssh_user = None
        ssh_key = None
        for key, val in params.items():
            if not is_string(key):
                raise ConfigError("Invalid configuration for server " + server_name)
            key = key.lower().strip()
            if key == "":
                pass
            if key == "ssh_user":
                if not is_string(val):
                    raise ConfigError("invalid 'ssh_user' parameter for server " + server_name)
                if not re.compile(r"^[a-z0-9]+$").match(val):
                    raise ConfigError("invalid 'ssh_user' parameter for server " + server_name)
                ssh_user = val
            elif key == "ssh_key":
                if not is_string(val):
                    raise ConfigError("invalid 'ssh_key' parameter for server " + server_name)
                if not os.path.exists(val):
                    raise ConfigError("no existing file for 'ssh_key' parameter for server " + server_name)
                ssh_key = val
            elif key == "prefix":
                if not is_string(val):
                    raise ConfigError("invalid 'prefix' parameter for server " + server_name)
                prefix = val
            elif key == "dest_folder":
                if not is_string(val):
                    raise ConfigError("invalid 'dest_folder' parameter for server " + server_name)
                if not os.path.isabs(val):
                    raise ConfigError("dest_folder for server " + server_name + " should be an absolute path")
                dest_folder = val
            else:
                raise RuntimeError("Unknown key " + key + " for server " + server_name)

        if prefix is None:
            prefix = server_name

        return prefix, dest_folder, ssh_user, ssh_key

    @staticmethod
    def _filename_to_prefix(file_path):
        result = re.sub(r'_+', "_", re.sub(r"[^a-zA-Z0-9]+", "_", file_path)).strip("_")
        return "file_exclude" if result == "exclude" else result

    @staticmethod
    def _db_to_prefix(db_info):
        db_name = db_info.split(":", 2)[0]
        return re.sub(r'_+', "_", re.sub(r"[^a-zA-Z0-9]+", "_", db_name)).strip("_")

    @staticmethod
    def _parse_file_action_conf(server_name, name, params, file_info, file_excludes):
        prefix, dest_folder, ssh_user, ssh_key = BackupConfig._parse_action_common(params, server_name)

        if file_info is None:
            raise ConfigError("Missing folder to backup for server " + server_name)
        if not is_string(file_info):
            raise ConfigError("invalid folder to backup for server " + server_name+": " + repr(file_info))
        if not file_info.startswith("/"):
            raise ConfigError("invalid folder to backup for server " + server_name+": " + file_info +
                              ": it's not an absolute path")
        if name is None:
            name = BackupConfig._filename_to_prefix(file_info)
        if not is_string(name):
            raise ConfigError("invalid prefix for folder " + file_info + " of server " + server_name + ": "
                              + repr(name))

        exclusions = []
        for to_exclude in file_excludes:
            if to_exclude.startswith(file_info):
                exclusions.append(to_exclude)
        return FileAction(server_name, prefix.strip("_"), name, dest_folder, ssh_user, ssh_key, file_info, exclusions)

    @staticmethod
    def _parse_db_action_conf(server_name, name, params, db_info, db_user):
        if db_info is None:
            raise ConfigError("Missing database information for server " + server_name)
        if not is_string(db_info):
            raise ConfigError("invalid database information for server " + server_name + ": " + repr(db_info))
        db_info_parts = db_info.split(":")
        if len(db_info_parts) != 3:
            raise ConfigError("invalid database information for server " + server_name + ": " + db_info)
        db_type = db_info_parts[0].strip().lower()
        if db_type not in ("mysql", "postgres", "mongo"):
            raise ConfigError("invalid database type " + db_type + " for server " + server_name)
        if not ll_int(db_info_parts[1].strip()):
            raise ConfigError("invalid database port for server " + server_name + ": " + db_info_parts[1])
        db_port = int(db_info_parts[1].strip())
        if 0 > db_port or db_port > 65534:
            raise ConfigError("invalid database port for server " + server_name + ": " + to_str(db_port))
        db_name = db_info_parts[2].strip()
        if not db_name:
            raise ConfigError("missing database name for server " + server_name + ": " + to_str(db_info))

        prefix, dest_folder, ssh_user, ssh_key = BackupConfig._parse_action_common(params, server_name)

        if name is None:
            name = BackupConfig._db_to_prefix(db_info)
        if not is_string(name):
            raise ConfigError("invalid prefix for database " + db_name + " of server " + server_name + ": "+repr(name))
        if db_type == "mysql":
            return MySqlAction(server_name, prefix.strip("_"), name, dest_folder, ssh_user, ssh_key,
                               db_user, db_name, db_port)
        elif db_type == "postgres":
            return PostgresAction(server_name, prefix.strip("_"), name, dest_folder, ssh_user, ssh_key,
                                  db_user, db_name, db_port)
        elif db_type == "mongo":
            return MongoDbAction(server_name, prefix.strip("_"), name, dest_folder, ssh_user, ssh_key,
                                 db_user, db_name, db_port)
        else:
            raise ConfigError("Unknown database type " + db_type + " for server " + server_name)

    @staticmethod
    def _parse_freq(config):
        """
        Parse a config value which should represent a frequency

        :param config: 	    The data to parse
        :type config:		any
        """
        day = BackupFrequency.NO_VALUE
        week = BackupFrequency.NO_VALUE
        month = BackupFrequency.NO_VALUE
        year = BackupFrequency.NO_VALUE
        if is_string(config):
            config = {config: BackupFrequency.ALL_STR}
        if not is_dict(config):
            return
        for freq_key, freq_val in config.items():
            if not is_string(freq_key):
                raise ConfigError("Invalid memory frequency " + to_str(freq_key))
            freq_key = to_str(freq_key).lower().strip()
            freq_val = to_str(freq_val).lower().strip()
            if freq_val == BackupFrequency.ALL_STR:
                freq_val = BackupFrequency.ALL_VALUE
            elif freq_val == BackupFrequency.NO_STR:
                freq_val = BackupFrequency.NO_VALUE
            elif not re.compile(r"^[0-9]+$").match(freq_val):
                raise ConfigError("Invalid memory frequency " + freq_val)

            if freq_key == BackupFrequency.FREQ_DAY:
                day = int(freq_val)
            elif freq_key == BackupFrequency.FREQ_WEEK:
                week = int(freq_val)
            elif freq_key == BackupFrequency.FREQ_MONTH:
                month = int(freq_val)
            elif freq_key == BackupFrequency.FREQ_YEAR:
                year = int(freq_val)
            else:
                raise ConfigError("Unknown memory frequency " + freq_key)
        return BackupFrequency(day, week, month, year)

    @staticmethod
    def _parse_syslog_report(config_file, report_config):
        template = None
        if is_string(report_config):
            template = BackupConfig._read_template(config_file, report_config)
        elif is_dict(report_config):
            for key, val in report_config.items():
                key = to_str(key).lower().strip()
                if key == "template":
                    if not BackupConfig._is_jinja2_installed():
                        raise ConfigError("Unable to use template, jinja2 is not installed")
                    if not is_string(val):
                        raise ConfigError("Invalid template field for syslog reporting: " + repr(val))
                    template = BackupConfig._read_template(config_file, val)
                else:
                    raise ConfigError("Invalid param " + key + " for syslog reporting")
        else:
            raise ConfigError("Invalid syslog report configuration: "+repr(report_config))
        return SyslogReportTarget(template)

    @staticmethod
    def _parse_email_report(config_file, report_config):
        if is_string(report_config):
            report_config = {"to": [report_config]}
        elif is_array(report_config):
            report_config = {"to": report_config}
        elif not is_dict(report_config):
            raise ConfigError("Invalid email configuration for report: " + repr(report_config))

        report_config = {to_str(k).strip().lower(): v for k, v in report_config.items()}
        if "to" not in report_config.keys():
            raise ConfigError("Missing 'to' parameter for email report configuration")
        if not is_array(report_config["to"]):
            report_config["to"] = [report_config["to"]]
        for email_address in report_config["to"]:
            if not is_string(email_address):
                raise ConfigError("Invalid email configuration for report: " + repr(email_address))
            if email.utils.parseaddr(email_address) == ('', ''):
                raise ConfigError("Invalid email address for report: "+repr(email_address))

        email_dest = report_config["to"]
        email_prefix = None
        email_sender = None
        email_user = None
        email_pwd = None
        email_server = None
        email_port = 25
        template = None
        del report_config["to"]
        for key, val in report_config.items():
            if not val:
                continue
            if key == "subject_prefix":
                if not is_string(val):
                    raise ConfigError("Invalid param " + key + " value for email reporting: " + repr(val))
                email_prefix = val
            elif key == "sender":
                if not is_string(val):
                    raise ConfigError("Invalid param " + key + " value for email reporting: " + repr(val))
                if email.utils.parseaddr(val) == ('', ''):
                    raise ConfigError("Invalid email sender address for report: " + repr(val))
                email_sender = val
            elif key == "smtp_user":
                if not is_string(val):
                    raise ConfigError("Invalid param " + key + " value for email reporting: " + repr(val))
                email_user = val
            elif key == "smtp_pwd":
                if not is_string(val):
                    raise ConfigError("Invalid param " + key + " value for email reporting: " + repr(val))
                email_pwd = val
            elif key == "smtp_server":
                if not is_string(val):
                    raise ConfigError("Invalid param " + key + " value for email reporting: " + repr(val))
                email_server = val
            elif key == "smtp_port":
                if not ll_int(val):
                    raise ConfigError("Invalid param " + key + " value for email reporting: " + repr(val))
                port = int(val)
                if 0 > port or port > 65534:
                    raise ConfigError("invalid smtp port for email reporting: " + to_str(port))
                email_port = port
            elif key == "template":
                if not BackupConfig._is_jinja2_installed():
                    raise ConfigError("Unable to use template, jinja2 is not installed")
                if not is_string(val):
                    raise ConfigError("Invalid template field for email report configuration: " + repr(val))
                template = BackupConfig._read_template(config_file, val)
            else:
                raise ConfigError("Invalid param " + key + " for email reporting")

        return EmailReportTarget(email_dest, email_prefix, email_sender, email_user, email_pwd,
                                 email_server, email_port, template)

    @staticmethod
    def _parse_file_report(config_file, report_config):
        file_names = []
        template = None
        if is_dict(report_config):
            for key, val in report_config.items():
                key = to_str(key).lower().strip()
                if key in ("output", "filename", "file"):
                    if not is_array(val) and not is_string(val):
                        raise ConfigError("Invalid "+key+" field for file report configuration: " + repr(val))
                    file_names.extend(to_list(val))
                elif key == "template":
                    if not BackupConfig._is_jinja2_installed():
                        raise ConfigError("Unable to use template, jinja2 is not installed")
                    if not is_string(val):
                        raise ConfigError("Invalid template field for file report configuration: " + repr(val))
                    template = BackupConfig._read_template(config_file, val)
                else:
                    raise ConfigError("Unknown "+key+" field for file report configuration")
        elif is_array(report_config):
            for val in report_config:
                if not is_string(val):
                    raise ConfigError("Invalid file report configuration: " + repr(val))
                file_names.append(val)
        elif is_string(report_config):
            file_names = [report_config]
        else:
            raise ConfigError("Invalid file report configuration: "+repr(report_config))
        return FileReportTarget(file_names, template)

    @staticmethod
    def _parse_webhook_report(config_file, report_config):
        try:
            import requests
        except ImportError:
            raise ConfigError("Unable to load python-requests, You can't use webhook reporting")

        if is_array(report_config):
            report_config = {'url': report_config}
        elif is_string(report_config):
            report_config = {'url': [report_config]}
        elif not is_dict(report_config):
            raise ConfigError("Invalid webhook configuration for report: " + repr(report_config))

        url_list = []
        params = None
        mapping_function = None
        post_method = None
        use_json = None
        for key, val in report_config.items():
            key = to_str(key).lower().strip()
            if key == "url":
                for url in to_list(val):
                    if not is_string(url):
                        raise ConfigError("Invalid 'url' field for webhook report configuration: "+repr(url))
                    url_list.append(url)
            elif key in ("params", "parameters"):
                if not is_dict(val):
                    raise ConfigError("Invalid '"+key+"' field for webhook report configuration: " + repr(val))
                params = val
            elif key == "mapping":
                if not is_string(val):
                    raise ConfigError("Invalid mapping field for webhook report configuration: " + repr(val))
                if "#" in val:
                    mapping_file, mapping_function = val.split("#", 2)
                else:
                    mapping_file = val
                with using_cwd(os.path.abspath(os.path.dirname(config_file))):
                    mapping_file = os.path.abspath(mapping_file)
                    if not os.path.exists(mapping_file):
                        raise ConfigError(
                            "mapping file not found for webhook report configuration: " + repr(mapping_file))
                mapping_function = MappingFunction(mapping_file, mapping_function)
            elif key == "method":
                if not is_string(val) or val.lower().strip() not in ("get", "post"):
                    raise ConfigError("Invalid 'method' field for webhook report configuration: " + repr(val))
                post_method = val.lower().strip() == "False"
            elif key == "json":
                if not ll_bool(val):
                    raise ConfigError("Invalid 'json' field for webhook report configuration: " + repr(val))
                use_json = to_bool(val)
            else:
                raise ConfigError("Unknown " + key + " field for file report configuration")

        # Autodetect remothod and json use from one another
        if use_json is None:
            if post_method is None:
                use_json = True
                post_method = True
            else:
                use_json = post_method
        elif post_method is None:
            post_method = True
        elif use_json and not post_method:
            raise ConfigError("You can't use json paramters on GET webhook report configuration")
        return WebhookReportTarget(url_list, params, post_method, use_json, mapping_function)

    @staticmethod
    def _read_template(config_file, template_file):
        path = os.path.abspath(os.path.dirname(config_file))
        with using_cwd(path):
            with open(os.path.abspath(template_file), "r") as fh:
                return fh.read()

    @staticmethod
    def _is_jinja2_installed():
        try:
            import jinja2
            return True
        except ImportError:
            return False


# Command functions
# ----------------------------------------------------------------------------

def glob_target(conf, identifier):
    """

    :param conf:
    :type conf:         BackupConfig
    :param identifier:
    :type identifier:   str
    :return:
    :rtype:             list[Action]
    """

    results = []
    actions = conf.get_actions()
    if ":" in identifier:
        first_part, second_part = identifier.split(":", 2)
        for action in actions:
            if fnmatch.fnmatch(action.server_name, first_part) or fnmatch.fnmatch(action.prefix, first_part):
                if fnmatch.fnmatch(action.name, second_part) or fnmatch.fnmatch(action.full_name, second_part):
                    results.append(action)
                elif isinstance(action, FileAction) and fnmatch.fnmatch(action.remote_folder, second_part):
                    results.append(action)
                elif isinstance(action, DbAction):
                    if fnmatch.fnmatch(action.database, second_part):
                        results.append(action)
                    elif fnmatch.fnmatch(action.db_type, second_part):
                        results.append(action)
    else:
        for action in actions:
            if fnmatch.fnmatch(action.server_name, identifier):
                results.append(action)
            elif fnmatch.fnmatch(action.name, identifier):
                results.append(action)
            elif fnmatch.fnmatch(action.full_name, identifier):
                results.append(action)
            elif fnmatch.fnmatch(action.prefix, identifier):
                results.append(action)
            elif isinstance(action, FileAction) and fnmatch.fnmatch(action.remote_folder, identifier):
                results.append(action)
            elif isinstance(action, DbAction):
                if fnmatch.fnmatch(action.database, identifier):
                    results.append(action)
                elif fnmatch.fnmatch(action.db_type, identifier):
                    results.append(action)
    if not results:
        raise RuntimeError("Unknown target "+identifier)
    return results


def glob_targets(conf, identifier_list, all_on_empty=True):
    """

    :param conf:
    :type conf:                 BackupConfig
    :param identifier_list:
    :type identifier_list:      list[str]
    :param all_on_empty:
    :type all_on_empty:         bool
    :return:
    :rtype:                     list[Action]
    """
    if not identifier_list and all_on_empty:
        return conf.get_actions()
    results = []
    for identifier in identifier_list:
        results.extend(glob_target(conf, identifier))
    return list(set(results))


def list_archives(action):
    """

    :param action:
    :type action:   Action
    """
    for storage in action.storage_list:
        archives = storage.list_archives(action.full_name)
        for archive in archives:
            if storage.should_keep(archive):
                log.info(archive + ": "+action.small_descr)
            else:
                log.info(archive+" [old]: "+action.small_descr)


def clean_archives(action):
    """

    :param action:
    :type action:   Action
    """
    for storage in action.storage_list:
        archives = storage.list_archives(action.full_name)
        for archive in archives:
            if not storage.should_keep(archive):
                storage.remove(archive)


def test_backup(actions):
    errors = []
    for action in actions:
        detected_errors = []
        try:
            detected_errors = action.check_src_access()
        except StandardError as e:
            errors.append([action, to_str(e)])

        for error in detected_errors:
            errors.append([action, to_str(error)])

        try:
            detected_errors = action.check_dest_access()
        except StandardError as e:
            errors.append([action, to_str(e)])
        for error in detected_errors:
            errors.append([action, to_str(error)])
    return errors


def test_reports(conf):
    """

    :param conf:
    :type conf:         BackupConfig
    """
    report = Report()
    report.add_warning("local", "Backup a test")

    for report_target in conf.report_list:
        try:
            report_target.report(report)
        except StandardError as e:
            log.error("Unable to send backup test report: "+to_str(e))


def do_report(conf, report):
    """

    :param conf:
    :type conf:         BackupConfig
    :param report:
    :type report:       Report
    """
    for report_target in conf.report_list:
        try:
            report_target.report(report)
        except StandardError as e:
            log.error("Unable to send backup test report: " + to_str(e))


def do_backup(action, report):
    """

    :param action:
    :type action:       Action
    :param report:
    :type report:       Report
    """
    try:
        clean_archives(action)
        action.run_backup()
    except StandardError as e:
        report.add_issue(action.server_name, "Unable to save data for "+action.small_descr+": "+to_str(e))
        log.exception(e)
    else:
        report.add_success(action.server_name, action.small_descr + " have been successfully backuped")


# Main function
# ----------------------------------------------------------------------------

def main():
    """
    Start the backup or display help

    :return:	0 if the software succeeded, a positive integer otherwise
    :rtype:		int
    """
    KillEventHandler.initialise()
    locale.setlocale(locale.LC_ALL, 'C')

    usage_str = '''Usage: python backup.py <command> [<args>]

        Allowed commands:
            run                     Backup the remote servers
            config                  Show the loaded configuration details 
            check                   Check read access to data to backup and write access to storage_list 
            check-reports           Send a test message to each report target
            list                    List existing backup
            clean                   Clean old backups
            
        Common optional arguments:
          -h, --help            show this help message and exit
          --config CONFIG, -c CONFIG
                                Specify a config file. Default: backup.config
          --log LOG, -l LOG     Specify a log file. You can specify 'stdout',
                                'stderr', 'syslog' or a file path. Default:
                                /var/log/backup.log
  
        '''
    target_str = """The servers to backup (Optional, default all).
You can specify targets using colon, use several targets using commas and the star character as joker.
Targets are """

    parser = argparse.ArgumentParser(description='Backup remote servers', usage=usage_str)
    parser.add_argument('command', help=argparse.SUPPRESS)
    if len(sys.argv) == 1:
        sys.argv = [sys.argv[0], "run"]

    args = parser.parse_args(sys.argv[1:2])
    if not args.command:
        sys.stderr.write('Unrecognized command' + os.linesep)
        sys.stderr.flush()
        parser.print_help()
        return 1

    if args.command == "config":
        usage_str = '''Usage: python backup.py config [options] [server:[target] [server:[target] ...]]'''
        parser = argparse.ArgumentParser(description='Show configuration details of the backup script',
                                         usage=usage_str)
        parser.add_argument('--config', '-c', default="backup.config",
                            help="Specify a config file. Default: backup.config")
        parser.add_argument('--log', '-l',
                            help="Specify a log file. " +
                                 "You can specify 'stdout', 'stderr', 'syslog' or a file path. " +
                                 "Default: /var/log/backup.log")
        parser.add_argument('target', nargs=argparse.REMAINDER, default=[], help=target_str)
        args = parser.parse_args(sys.argv[2:])
        init_log(args.log)

        config_file = args.config
        if not os.path.isabs(config_file) and not os.path.exists(config_file):
            config_file = os.path.join(script_path, config_file)

        if not os.path.exists(config_file):
            log.error("Unable to locate config file "+args.config)
            return 1

        try:
            conf = BackupConfig(config_file)
            try:
                targets = glob_targets(conf, args.target)
            except RuntimeError as e:
                parser.error(e)
                return 1
            conf.show(targets)
        except KeyboardInterrupt:
            log.warning("Aborted.")
            return 0
        except ConfigError as e:
            log.error("Configuration file "+os.path.abspath(config_file)+" is invalid:" + os.linesep + to_str(e))
            return 1
        except StandardError as e:
            log.error(to_str(e))
            return 1
    elif args.command == "check":
        usage_str = '''Usage: python backup.py check [options] [server[:target,target2,...] [server[:target] ...]]'''
        parser = argparse.ArgumentParser(description='Test the access to source data and backup destinations',
                                         usage=usage_str)
        parser.add_argument('--config', '-c', default="backup.config",
                            help="Specify a config file. Default: backup.config")
        parser.add_argument('--log', '-l',
                            help="Specify a log file. " +
                                 "You can specify 'stdout', 'stderr', 'syslog' or a file path. " +
                                 "Default: /var/log/backup.log")
        parser.add_argument('target', nargs=argparse.REMAINDER, default=[], help=target_str)
        args = parser.parse_args(sys.argv[2:])
        init_log(args.log)

        config_file = args.config
        if not os.path.isabs(config_file) and not os.path.exists(config_file):
            config_file = os.path.join(script_path, config_file)

        if not os.path.exists(config_file):
            log.error("Unable to locate config file " + args.config)
            return 1

        try:
            conf = BackupConfig(config_file)
            try:
                actions = glob_targets(conf, args.target)
            except RuntimeError as e:
                parser.error(e)
                return 1

            actions_by_server = index_by(actions, lambda a: a.server_name)
            error_count = 0
            for server_name, targets in actions_by_server.items():
                error_list = test_backup(targets)
                error_count = error_count + len(error_list)
                if error_list:
                    error_text = server_name + ":"
                    for action, error in error_list:
                        error_text += os.linesep + indent(to_str(action.prefix) + ": " + to_str(error))
                    log.error(error_text)
            if error_count:
                log.info(to_str(error_count) + " error detected.")
                return 3
            else:
                log.info("Connectivity check completed successfully.")
        except KeyboardInterrupt:
            log.warning("Aborted.")
            return 0
        except ConfigError as e:
            log.error("Configuration file "+os.path.abspath(config_file)+" is invalid:" + os.linesep + to_str(e))
            return 1
        except StandardError as e:
            log.error(to_str(e))
            return 1
    elif args.command == "list":
        usage_str = '''Usage: python backup.py list [options] [server[:target,target2,...] [server[:target] ...]]'''
        parser = argparse.ArgumentParser(description='List backup achives', usage=usage_str)
        parser.add_argument('--config', '-c', default="backup.config",
                            help="Specify a config file. Default: backup.config")
        parser.add_argument('--log', '-l',
                            help="Specify a log file. " +
                                 "You can specify 'stdout', 'stderr', 'syslog' or a file path. " +
                                 "Default: /var/log/backup.log")
        parser.add_argument('target', nargs=argparse.REMAINDER, default=[], help=target_str)
        args = parser.parse_args(sys.argv[2:])
        init_log(args.log)

        config_file = args.config
        if not os.path.isabs(config_file) and not os.path.exists(config_file):
            config_file = os.path.join(script_path, config_file)

        if not os.path.exists(config_file):
            log.error("Unable to locate config file " + args.config)
            return 1

        try:
            conf = BackupConfig(config_file)
            try:
                actions = glob_targets(conf, args.target)
            except RuntimeError as e:
                parser.error(e)
                return 1
            for action in actions:
                list_archives(action)
        except KeyboardInterrupt:
            log.warning("Aborted.")
            return 0
        except ConfigError as e:
            log.error("Configuration file "+os.path.abspath(config_file)+" is invalid:" + os.linesep + to_str(e))
            return 1
        except StandardError as e:
            log.error(to_str(e))
            return 1
    elif args.command == "clean":
        usage_str = '''Usage: python backup.py clean [options] [server[:target,target2,...] [server[:target] ...]]'''
        parser = argparse.ArgumentParser(description='Clean old backup achives', usage=usage_str)
        parser.add_argument('--config', '-c', default="backup.config",
                            help="Specify a config file. Default: backup.config")
        parser.add_argument('--log', '-l',
                            help="Specify a log file. " +
                                 "You can specify 'stdout', 'stderr', 'syslog' or a file path. " +
                                 "Default: /var/log/backup.log")
        parser.add_argument('target', nargs=argparse.REMAINDER, default=[], help=target_str)
        args = parser.parse_args(sys.argv[2:])
        init_log(args.log)

        config_file = args.config
        if not os.path.isabs(config_file) and not os.path.exists(config_file):
            config_file = os.path.join(script_path, config_file)

        if not os.path.exists(config_file):
            log.error("Unable to locate config file " + args.config)
            return 1

        try:
            conf = BackupConfig(config_file)
            try:
                actions = glob_targets(conf, args.target)
            except RuntimeError as e:
                parser.error(e)
                return 1
            for action in actions:
                clean_archives(action)
        except KeyboardInterrupt:
            log.warning("Aborted.")
            return 0
        except ConfigError as e:
            log.error("Configuration file "+os.path.abspath(config_file)+" is invalid:" + os.linesep + to_str(e))
            return 1
        except StandardError as e:
            log.error(to_str(e))
            return 1
    elif args.command == "check-reports":
        usage_str = '''Usage: python backup.py check [options] [server[:target,target2,...] [server[:target] ...]]'''
        parser = argparse.ArgumentParser(description='Test the access to source data and backup destinations',
                                         usage=usage_str)
        parser.add_argument('--config', '-c', default="backup.config",
                            help="Specify a config file. Default: backup.config")
        parser.add_argument('--log', '-l',
                            help="Specify a log file. " +
                                 "You can specify 'stdout', 'stderr', 'syslog' or a file path. " +
                                 "Default: /var/log/backup.log")
        args = parser.parse_args(sys.argv[2:])
        init_log(args.log)

        config_file = args.config
        if not os.path.isabs(config_file) and not os.path.exists(config_file):
            config_file = os.path.join(script_path, config_file)

        if not os.path.exists(config_file):
            log.error("Unable to locate config file " + args.config)
            return 1

        try:
            conf = BackupConfig(config_file)
            test_reports(conf)
        except KeyboardInterrupt:
            log.warning("Aborted.")
            return 0
        except ConfigError as e:
            log.error("Configuration file "+os.path.abspath(config_file)+" is invalid:" + os.linesep + to_str(e))
            return 1
        except StandardError as e:
            log.error(to_str(e))
            return 1
    elif args.command == "run":
        usage_str = '''Usage: python backup.py run [options] [server[:target,target2,...] [server[:target] ...]]'''
        parser = argparse.ArgumentParser(description='Run backups', usage=usage_str)
        parser.add_argument('--config', '-c', default="backup.config",
                            help="Specify a config file. Default: backup.config")
        parser.add_argument('--log', '-l',
                            help="Specify a log file. " +
                                 "You can specify 'stdout', 'stderr', 'syslog' or a file path. " +
                                 "Default: /var/log/backup.log")
        parser.add_argument('target', nargs=argparse.REMAINDER, default=[], help=target_str)
        args = parser.parse_args(sys.argv[2:])
        init_log(args.log)

        config_file = args.config
        if not os.path.isabs(config_file) and not os.path.exists(config_file):
            config_file = os.path.join(script_path, config_file)

        if not os.path.exists(config_file):
            log.error("Unable to locate config file " + args.config)
            return 1

        try:
            conf = BackupConfig(config_file)
            try:
                actions = glob_targets(conf, args.target)
            except RuntimeError as e:
                parser.error(e)
                return 1
            report = Report()
            for action in actions:
                do_backup(action, report)
            do_report(conf, report)
        except KeyboardInterrupt:
            log.warning("Backup aborted. Sending reports...")
    else:
        sys.stderr.write('Unrecognized command ' + to_str(sys.argv[1]) + os.linesep)
        sys.stderr.flush()
        parser.print_help()
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
