import aioboto3

class S3:
    def __init__(self):
        self.session = aioboto3.Session()
