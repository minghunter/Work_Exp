import os
import gc


class CleanupFiles:
    def __init__(self, lstFileName, logger):
        self.lstFileName = lstFileName
        self.logger = logger

    def cleanup(self):
        for f in self.lstFileName:
            if f:
                self.logger.warning('remove %s' % f)
                os.remove(f)

        gc.collect()