class IllegalFileNameError(ValueError):
    def __init__(self, filename, *args):
        super().__init__(filename, *args)
        self.filename = filename
