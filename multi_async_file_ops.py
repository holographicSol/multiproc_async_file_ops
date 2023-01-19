""" Written by Benjamin Jack Cullen
Intention: File ops template.
Setup: Multiprocess + Async.
"""
import os
import time
from datetime import datetime
import magic
import codecs
import asyncio
from aiomultiprocess import Pool
import prescan
import chunk_handler


def file_sub_ops(file: str) -> str:
    """ optional sub-ops """
    global _buffer
    try:
        buff = magic.from_buffer(codecs.open(file, "rb").read(int(1024)))
    except:
        buff = magic.from_buffer(open(file, "r").read(int(1024)))
    return buff


async def file_ops(file: str) -> list:
    """ ops """
    try:
        return [file, await asyncio.to_thread(file_sub_ops, file)]
    except:
        pass


async def entry_point(chunk: list) -> list:
    return [await file_ops(item) for item in chunk]


async def main(_chunks: list) -> list:
    async with Pool() as pool:
        _results = await pool.map(entry_point, _chunks)
    return _results


def pre_scan_handler(_target: str) -> list:
    scan_results = prescan.scan(path=_target)
    _files = scan_results[0]
    _x_files = scan_results[1]
    return _files, _x_files


def logger(*args, fname: str, _dt: str):
    target_dir = './data/'+_dt+'/'
    if not os.path.exists('./data/'):
        os.mkdir('./data/')
    if not os.path.exists(target_dir):
        os.mkdir(target_dir)
    fname = target_dir+fname
    codecs.open(fname, "w", encoding='utf8').close()
    codecs.open(fname, "a", encoding='utf8').write('\n'.join(str(arg) for arg in args))


if __name__ == '__main__':
    _target = 'D:\\Pictures\\'
    _proc_max = 8

    dt = str(datetime.now()).replace(':', '-').replace('.', '-').replace(' ', '_')
    print(dt)

    t = time.perf_counter()
    files, x_files = pre_scan_handler(_target=_target)
    print('[pre-scan time]', time.perf_counter() - t)

    # uncomment for logging
    logger(*files, fname='pre_scan_files_'+dt+'.txt', _dt=dt)
    logger(*x_files, fname='pre_scan_x_files_'+dt+'.txt', _dt=dt)

    print('[files]', len(files))
    print('[x_files]', len(x_files))

    chunks = chunk_handler.chunk_data(files, _proc_max)
    print('[number of expected chunks]', len(chunks))

    t = time.perf_counter()
    results = asyncio.run(main(chunks))
    print('[chunked results]', len(results))
    print('[multi-process+async ops time]', time.perf_counter()-t)

    results = chunk_handler.un_chunk_data(results, depth=1)

    # uncomment for logging
    logger(*results, fname='results_'+dt+'.txt', _dt=dt)

    print('[complete]')
