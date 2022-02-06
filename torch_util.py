from .logging_util import info as _log_info
from .logging_util import warn as _log_warn
import typing
import math
import numpy as np
import torch
import torch.backends.cudnn
import random

_device: typing.Optional[torch.device] = None


def set_seed(seed: int):
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)
    np.random.seed(seed)
    random.seed(seed)
    torch.backends.cudnn.deterministic = True


def set_device(device: str):
    global _device

    if torch.cuda.is_available():
        _device = torch.device(device)
        _log_info(f'Set device as {_device}')
    else:
        _device = torch.device('cpu')
        _log_warn('Cuda is not available!')


def to_device(obj):
    if not _device:
        raise AssertionError
    return obj.to(device=_device)


def to_tensor(obj) -> torch.Tensor:
    if isinstance(obj, int):
        out_tensor = torch.tensor(obj, dtype=torch.int64)
    elif isinstance(obj, float):
        out_tensor = torch.tensor(obj, dtype=torch.float32)
    elif isinstance(obj, torch.Tensor):
        out_tensor = obj
    elif isinstance(obj, (list, tuple)):
        out_tensor = torch.tensor(obj)
    elif isinstance(obj, np.ndarray):
        if obj.dtype == np.float32:
            out_tensor = torch.from_numpy(obj)
        elif obj.dtype in (np.float16, np.float32, np.float64):
            out_tensor = torch.tensor(obj, dtype=torch.float32)
        else:
            out_tensor = torch.tensor(obj, dtype=torch.int64)
    else:
        raise AssertionError

    if not _device:
        return out_tensor
    else:
        return to_device(out_tensor)


def to_numpy(obj: typing.Union[torch.Tensor, np.ndarray]) -> np.ndarray:
    if isinstance(obj, np.ndarray):
        return obj
    elif isinstance(obj, torch.Tensor):
        return obj.detach().cpu().numpy()
    else:
        raise AssertionError


def idx_batch_generator(num_samples: int, batch_size: int, discard_remain: bool = False) -> typing.Iterator[np.ndarray]:
    shuffled_indices = np.random.permutation(num_samples)

    start_pos = 0
    end_pos = batch_size
    while start_pos < num_samples:
        indices = shuffled_indices[start_pos: end_pos]

        if discard_remain and len(indices) < batch_size:
            break

        yield indices

        start_pos += batch_size
        end_pos += batch_size


def src_tgt_idx_batch_generator(num_samples_S: int,
                                num_samples_T: int,
                                batch_size: int) -> typing.Iterator[tuple[np.ndarray, np.ndarray]]:
    union_num_samples = math.ceil(max(num_samples_S, num_samples_T) / batch_size) * batch_size

    src_idxs = np.random.permutation(num_samples_S)
    tgt_idxs = np.random.permutation(num_samples_T)
    src_idxs = np.concatenate(
        [src_idxs, np.random.choice(src_idxs, union_num_samples - num_samples_S, replace=True)])
    tgt_idxs = np.concatenate(
        [tgt_idxs, np.random.choice(tgt_idxs, union_num_samples - num_samples_T, replace=True)])

    assert len(src_idxs) == len(tgt_idxs) == union_num_samples
    assert union_num_samples % batch_size == 0

    for batch_idxs in idx_batch_generator(num_samples=union_num_samples, batch_size=batch_size):
        yield src_idxs[batch_idxs], tgt_idxs[batch_idxs]
