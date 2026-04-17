# -*- coding: utf-8 -*-
"""Modulo para conexão com FTP"""

import ftplib
import ssl


class ImplicitFtpTls(ftplib.FTP_TLS):
    """FTP_TLS subclass that automatically wraps sockets in SSL to support implicit FTPS."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sock = None

    @property
    def sock(self):
        """Return the socket."""
        return self._sock

    @sock.setter
    def sock(self, value):
        """When modifying the socket, ensure that it is ssl wrapped."""
        if value is not None and not isinstance(value, ssl.SSLSocket):
            value = self.context.wrap_socket(value)
        self._sock = value


def connect_ftp(
    host: str,
    port: int,
    username: str,
    password: str,
    secure: bool = True,
):
    """Connect to FTP

    Returns:
        ImplicitFTP_TLS: ftp client
    """

    if secure:
        ftp_client = ImplicitFtpTls()
    else:
        ftp_client = ftplib.FTP()
    ftp_client.connect(host=host, port=port)
    ftp_client.login(user=username, passwd=password)
    if secure:
        ftp_client.prot_p()
    return ftp_client
