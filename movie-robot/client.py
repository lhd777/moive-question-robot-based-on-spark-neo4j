# 服务端
import sys
import json
import requests
from PyQt5.QtNetwork import QTcpServer, QHostAddress
from PyQt5.QtWidgets import QApplication, QWidget, QTextBrowser, QVBoxLayout
from neo4j import *
from ModelProcess import *
import pickle


class Server(QWidget):
    def __init__(self, model, prediction, vocabulary):
        super(Server, self).__init__()
        self.model = model
        self.prediction = prediction
        self.vocabulary = vocabulary
        self.resize(500, 450)

        # 1
        self.browser = QTextBrowser(self)

        self.v_layout = QVBoxLayout()
        self.v_layout.addWidget(self.browser)
        self.setLayout(self.v_layout)

        # 2
        self.server = QTcpServer(self)
        if not self.server.listen(QHostAddress.LocalHost, 8080):
            self.browser.append(self.server.errorString())
        self.server.newConnection.connect(self.new_socket_slot)

    def new_socket_slot(self):
        sock = self.server.nextPendingConnection()

        peer_address = sock.peerAddress().toString()
        peer_port = sock.peerPort()
        news = 'Connected with address {}, port{}'.format(peer_address, str(peer_port))
        self.browser.append(news)

        sock.readyRead.connect(lambda: self.read_data_slot(sock))
        sock.disconnected.connect(lambda: self.disconnected_slot(sock))

    # 3
    def read_data_slot(self, sock):
        while sock.bytesAvailable():
            datagram = sock.read(sock.bytesAvailable())
            message = datagram.decode()
            answer = self.get_answer(message).encode()
            sock.write(bytes(answer))

    def get_answer(self, message):
        sentence = pre_dosegment(message)
        sentence = ' '.join(sentence)
        prediction1 = self.model.test(str(sentence), self.vocabulary)
        query = match_question(prediction1, str(message))
        answer = self.prediction.run(query, prediction1)
        return answer

    # 4
    def disconnected_slot(self, sock):
        peer_address = sock.peerAddress().toString()
        peer_port = sock.peerPort()
        news = 'Disconnected with address {}, port {}'.format(peer_address, str(peer_port))
        self.browser.append(news)

        sock.close()


if __name__ == '__main__':
    app = QApplication(sys.argv)
    prediction = predict(test_graph)
    model = NaiveBayesModelMe()
    pkl_file = open("vocabulary.pkl", "rb")
    vocabulary = pickle.load(pkl_file)
    demo = Server(model, prediction, vocabulary)
    demo.show()
    sys.exit(app.exec_())
