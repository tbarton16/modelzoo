import argparse
import pickle
import queue
import time
from collections import defaultdict
from glob import glob
from multiprocessing import Process, Queue

from datasketch.lean_minhash import LeanMinHash
fp = '/tmp/dedupstream/RedPajama_norm/arxiv3/minhash_nfc/0-0.pickle'
with open(fp, "rb") as fin:
    for item in pickle.load(fin):
        key = f"{item['file_name']}@{item['doc_id']}"
        minhash = LeanMinHash(item["hash"])
        print(key, min(minhash.hashvalues))
        # for i, doc_queue in enumerate(doc_queues):
        #     H = _H(minhash.hashvalues[i * r : (i + 1) * r])
        #     doc_queue.put((key, H))