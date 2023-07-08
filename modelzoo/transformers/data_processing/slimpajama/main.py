import argparse
import os
import sys

sys.path.append("./preprocessing")
from preprocessing import normalize_text, filter, shuffle_holdout, datasets
from dedup import to_hash, dedup_train, generate_duplicate_pairs, generate_connected_components, generate_duplicates_dict
from streaming.base.dataset import StreamingDataset
from datasketch.lsh import _optimal_param
# ds_names = ["arxiv", "stackexchange", "book", "wikipedia", "github", "c4", "common_crawl"]
# cc_years = ["2019-30", "2020-05", "2021-04", "2022-05" "2023-06"]
ds_names = ['arxiv3']
def main(input_dir):

    # norm text
    ds_dirs = ds_names.copy()
    
    red_pj_norm = os.path.join(input_dir, "RedPajama_norm")
    # for dataset in ds_dirs:
    #     norm_args = argparse.Namespace()
    #     norm_args.data_dir = os.path.join(input_dir, dataset)
    #     norm_args.target_dir = os.path.join(red_pj_norm, dataset)
    #     print("norm dir ", norm_args.target_dir)
    #     norm_args.zst = "common_crawl" in dataset
    #     norm_args.idx = -1
    #     normalize_text.normalize_text(norm_args)
    print("norm done")
    # filter docs
    # short_docs = os.path.join(red_pj_norm, "red_pj_filter.pickle")
    # filter_args = argparse.Namespace()
    # filter_args.input_dir = red_pj_norm
    # filter_args.output_file = short_docs
    # filter_args.n_docs = 100 #TODO: update
    # filter_args.dataset_name = "all"
    # filter_args.threshold = 200
    # filter.filter_dataset(filter_args)
    # print("filter done")
    # generate minhash
    # for dataset in ds_dirs:
    #     hash_args = argparse.Namespace()
    #     hash_args.dataset_name = "common_crawl" if "common_crawl" in dataset else dataset
    #     hash_args.input_dir = os.path.join(red_pj_norm, dataset)
    #     hash_args.output_dir = os.path.join(red_pj_norm, dataset)
    #     print("hash dir ", hash_args.output_dir)
    #     hash_args.n_docs = 200 #TODO: update
    #     hash_args.iter = 0
    #     hash_args.index_start = 0
    #     hash_args.index_end = None
    #     hash_args.w = 13
    #     hash_args.k = 10000
    #     to_hash.generate_hashes(hash_args)
    print("hash done")
    # generate duplicates
    b, r = _optimal_param(.9, 128, .5, .5)
    dup_dir = os.path.join(red_pj_norm, "dup")
    os.makedirs(dup_dir, exist_ok=True)
    dup_pairs_args = argparse.Namespace()
    dup_pairs_args.input_dir = red_pj_norm
    dup_pairs_args.out_file = os.path.join(dup_dir, "duplicate_pairs.txt")
    dup_pairs_args.range = r
    dup_pairs_args.bands = b
    print(r,b)
    dup_pairs_args.processes = 64 - b

    #dup_pairs_args.range = 13
    #dup_pairs_args.bands = 9
    #dup_pairs_args.processes = 45
    generate_duplicate_pairs.generate_pairs(dup_pairs_args)

    dup_connected_args = argparse.Namespace()
    dup_connected_args.input_dir = dup_dir
    dup_connected_args.out_file = os.path.join(dup_dir, "connected_components.pickle")
    generate_connected_components.generate_connected_components_mp(dup_connected_args)

    dup_docs = os.path.join(dup_dir, "duplicates.pickle")
    dup_dict_args = argparse.Namespace()
    dup_dict_args.input_file = os.path.join(dup_dir, "connected_components.pickle")
    dup_dict_args.out_file = dup_docs
    generate_duplicates_dict.generate_duplicates(dup_dict_args)

    # # interleave & shuffle
    # shuffle_holdout.pass_1_shuffle(
    #     datasets.RedPajamaReplication(
    #         datasets.redpj_datasets(red_pj_norm+"/"), dup_docs, short_docs
    #     ),
    #     output_dir_path=os.path.join(red_pj_norm, "pass1"),
    # )
    

    # # split train & holdout
    # for j in range(1, 21):
    #     shuffle_holdout.pass_2_shuffle_holdout(
    #         input_dir_path=os.path.join(red_pj_norm, "pass1"),
    #         output_dir_path=os.path.join(red_pj_norm, "train"),
    #         output_holdout_dir_path=os.path.join(red_pj_norm, "holdout"),
    #         start_index=j-1,
    #         end_index=j,
    #         chunk_id=j,
    #     )

    # # Deduplicate Train against Holdout
    # for j in range(1, 21):
    #     dedup_train.deduplicate_train_holdout_sets(
    #         os.path.join(red_pj_norm, "train"), os.path.join(red_pj_norm, "holdout"), os.path.join(red_pj_norm, "train_deduped"), j,
    #     )

if __name__ == "__main__":
    q = 'oci://mosaicml-internal-datasets/stack-split/javascript'
    # streamer = StreamingDataset(local='/tmp/dedupstream', remote=q, 
    #                             shuffle=False, predownload=True)
    # os.makedirs('/tmp/dedupstream/redpj', exist_ok=True)
    # os.makedirs('/tmp/dedupstream/redpj/javascript', exist_ok=True)
    # import json
    # for _file in range(100):
    #     with open(f'/tmp/dedupstream/{_file}.jsonl', 'w') as f:
    #         doc = streamer[_file]["text"]
    #         record = {"text": doc , "meta":str(_file)}
    #         f.write(json.dumps(record) + "\n")
    #         f.flush()
    parser = argparse.ArgumentParser()
    parser.add_argument("input_dir", help="Dataset input directory.")
    args = parser.parse_args()
    main(args.input_dir)
