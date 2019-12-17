# 客户端
import sys
from PyQt5.QtCore import Qt
from PyQt5.QtNetwork import QTcpSocket, QHostAddress
from PyQt5.QtWidgets import QApplication, QWidget, QTextBrowser, QTextEdit, QSplitter, QPushButton, \
    QHBoxLayout, QVBoxLayout


class Client(QWidget):
    def __init__(self):
        super(Client, self).__init__()
        self.resize(500, 450)
        # 1
        self.browser = QTextBrowser(self)
        self.edit = QTextEdit(self)

        self.splitter = QSplitter(self)
        self.splitter.setOrientation(Qt.Vertical)
        self.splitter.addWidget(self.browser)
        self.splitter.addWidget(self.edit)
        self.splitter.setSizes([350, 100])

        self.send_btn = QPushButton('Send', self)
        self.close_btn = QPushButton('Close', self)

        self.h_layout = QHBoxLayout()
        self.v_layout = QVBoxLayout()

        # 2
        self.sock = QTcpSocket(self)
        self.sock.connectToHost(QHostAddress.LocalHost, 8080)

        self.layout_init()
        self.signal_init()

    def layout_init(self):
        self.h_layout.addStretch(1)
        self.h_layout.addWidget(self.close_btn)
        self.h_layout.addWidget(self.send_btn)
        self.v_layout.addWidget(self.splitter)
        self.v_layout.addLayout(self.h_layout)
        self.setLayout(self.v_layout)

    def signal_init(self):
        self.send_btn.clicked.connect(self.write_data_slot)  # 3
        self.close_btn.clicked.connect(self.close_slot)  # 4
        self.sock.connected.connect(self.connected_slot)  # 5
        self.sock.readyRead.connect(self.read_data_slot)  # 6

    def write_data_slot(self):
        message = self.edit.toPlainText()
        self.browser.append('Client: {}'.format(message))
        datagram = message.encode()
        self.sock.write(datagram)
        self.edit.clear()

    def connected_slot(self):
        message = '小新：电影问答机器人小新为您服务，有什么关于电影方面的问题都可以问我噢(>^ω^<)'
        self.browser.append(message)

    def read_data_slot(self):
        while self.sock.bytesAvailable():
            datagram = self.sock.read(self.sock.bytesAvailable())
            message = datagram.decode()
            self.browser.append('小新: {}'.format(message))

    def close_slot(self):
        self.sock.close()
        self.close()

    def closeEvent(self, event):
        self.sock.close()
        event.accept()


if __name__ == '__main__':
    app = QApplication(sys.argv)
    demo = Client()
    demo.show()
    sys.exit(app.exec_())
