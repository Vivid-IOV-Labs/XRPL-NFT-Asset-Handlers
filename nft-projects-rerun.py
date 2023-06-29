import json
import sys
import math

from utils import chunks, read_json
from config import Config
from engine import Engine, AsyncS3FileWriter

async def metadata_check(data, completed):
    try:
        path = f"assets/metadata/{data['nft_token_id']}/metadata.json"
        await read_json(Config.DATA_DUMP_BUCKET, path, Config)
        print(f"metadata-exists for {data['nft_token_id']}")
        completed[data['nft_token_id']] = True
        return None
    except Exception as _:
        return data


async def check_for_metadata():
    tracked_nfts = json.load(open("tracked-nfts.json", "r"))
    completed = json.load(open("completed-reruns.json", "r"))
    to_check = [data for data in tracked_nfts if completed.get(data["nft_token_id"], False) is False]
    final_result = json.load(open("no-metadata.json", "r"))
    batch = 1
    for chunk in chunks(to_check, 100):
        if len(final_result) >= 10000:
            break
        print(f"Starting Batch {batch}")
        results = await asyncio.gather(*[metadata_check(dat, completed) for dat in chunk])
        final_result.extend([result for result in results if result is not None])
        json.dump(final_result, open("no-metadata.json", "w"), indent=2)
        json.dump(completed, open("completed-reruns.json", "w"), indent=2)
        print(f"Done with Batch {batch}")
        batch += 1


async def rerun():
    completed = json.load(open("completed-reruns.json", "r"))
    errors = json.load(open("rerun-errors.json", "r"))
    no_metadata_objects = json.load(open("no-metadata.json", "r"))

    writer = AsyncS3FileWriter()

    to_rerun = [data for data in no_metadata_objects if completed.get(data["nft_token_id"], False) is False]

    batch = 1
    runs_per_batch = 10
    batch_count = math.ceil(len(to_rerun)/runs_per_batch)
    print(f"Total IDs: {len(to_rerun)}\nRuns Per Batch: {runs_per_batch}\nTotal Batches: {batch_count}\n")
    for chunk in chunks(to_rerun, runs_per_batch):
        print(f"Started Batch {batch}")
        await asyncio.gather(*[Engine({}).retry_v2(data["nft_token_id"], data["uri"], data["issuer"], completed, errors) for data in chunk])
        json.dump(completed, open("completed-reruns.json", "w"), indent=2)
        json.dump(errors, open("rerun-errors.json", "w"), indent=2)
        await writer.write_json("rerun/rerun_errors.json", errors)
        print(f"Completed Batch {batch} Out of {batch_count} Batches")
        batch += 1

if __name__ == "__main__":
    import asyncio

    stage = sys.argv[1]
    if stage == "metadata-check":
        asyncio.run(check_for_metadata())
    elif stage == "rerun":
        asyncio.run(rerun())
    elif stage == "purge-completed":
        complete = json.load(open("completed-reruns.json", "r"))
        tracked_nfts = json.load(open("tracked-nfts.json", "r"))

        new_tracked = [tracked for tracked in tracked_nfts if complete.get(tracked["nft_token_id"], False) is False]
        json.dump(new_tracked, open("tracked-nfts.json", "w"), indent=2)
        json.dump({}, open("completed-reruns.json", "w"), indent=2)
        json.dump([], open("no-metadata.json", "w"), indent=2)
        print(f"Purged: {len(complete.keys())}\nFormer Tracked Count: {len(tracked_nfts)}\nNew Tracked: {len(new_tracked)}")
