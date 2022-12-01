# A datastructure to maintain the "gossip" sent and "gossip" recieved Info
class MessageList():
    """
    More fields will be added as per requirements
    """

    def __init__(self):
        self.message_dict = {}

    def put(self, message_hash):
        self.message_dict[message_hash] = True

    def get(self, message_hash):
        if message_hash in self.message_dict:
            return True
        else:
            return False