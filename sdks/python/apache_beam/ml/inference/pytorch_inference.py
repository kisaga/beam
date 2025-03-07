#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# pytype: skip-file

from collections import defaultdict
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Sequence

import torch
from apache_beam.io.filesystems import FileSystems
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult


def _load_model(
    model_class: torch.nn.Module, state_dict_path, device, **model_params):
  model = model_class(**model_params)
  model.to(device)
  file = FileSystems.open(state_dict_path, 'rb')
  model.load_state_dict(torch.load(file))
  model.eval()
  return model


def _convert_to_device(examples: torch.Tensor, device) -> torch.Tensor:
  """
  Converts samples to a style matching given device.

  **NOTE:** A user may pass in device='GPU' but if GPU is not detected in the
  environment it must be converted back to CPU.
  """
  if examples.device != device:
    examples = examples.to(device)
  return examples


class PytorchModelHandlerTensor(ModelHandler[torch.Tensor,
                                             PredictionResult,
                                             torch.nn.Module]):
  def __init__(
      self,
      state_dict_path: str,
      model_class: Callable[..., torch.nn.Module],
      model_params: Dict[str, Any],
      device: str = 'CPU'):
    """Implementation of the ModelHandler interface for PyTorch.

    Example Usage::

      pcoll | RunInference(PytorchModelHandlerTensor(state_dict_path="my_uri"))

    Args:
      state_dict_path: path to the saved dictionary of the model state.
      model_class: class of the Pytorch model that defines the model
        structure.
      device: the device on which you wish to run the model. If
        ``device = GPU`` then a GPU device will be used if it is available.
        Otherwise, it will be CPU.

    See https://pytorch.org/tutorials/beginner/saving_loading_models.html
    for details
    """
    self._state_dict_path = state_dict_path
    if device == 'GPU' and torch.cuda.is_available():
      self._device = torch.device('cuda')
    else:
      self._device = torch.device('cpu')
    self._model_class = model_class
    self._model_params = model_params

  def load_model(self) -> torch.nn.Module:
    """Loads and initializes a Pytorch model for processing."""
    return _load_model(
        self._model_class,
        self._state_dict_path,
        self._device,
        **self._model_params)

  def run_inference(
      self,
      batch: Sequence[torch.Tensor],
      model: torch.nn.Module,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """
    Runs inferences on a batch of Tensors and returns an Iterable of
    Tensor Predictions.

    This method stacks the list of Tensors in a vectorized format to optimize
    the inference call.

    Args:
      batch: A sequence of Tensors. These Tensors should be batchable, as this
        method will call `torch.stack()` and pass in batched Tensors with
        dimensions (batch_size, n_features, etc.) into the model's forward()
        function.
      model: A PyTorch model.
      inference_args: Non-batchable arguments required as inputs to the model's
        forward() function. Unlike Tensors in `batch`, these parameters will
        not be dynamically batched

    Returns:
      An Iterable of type PredictionResult.
    """
    inference_args = {} if not inference_args else inference_args

    batched_tensors = torch.stack(batch)
    batched_tensors = _convert_to_device(batched_tensors, self._device)
    predictions = model(batched_tensors, **inference_args)
    return [PredictionResult(x, y) for x, y in zip(batch, predictions)]

  def get_num_bytes(self, batch: Sequence[torch.Tensor]) -> int:
    """
    Returns:
      The number of bytes of data for a batch of Tensors.
    """
    return sum((el.element_size() for tensor in batch for el in tensor))

  def get_metrics_namespace(self) -> str:
    """
    Returns:
       A namespace for metrics collected by the RunInference transform.
    """
    return 'RunInferencePytorch'


class PytorchModelHandlerKeyedTensor(ModelHandler[Dict[str, torch.Tensor],
                                                  PredictionResult,
                                                  torch.nn.Module]):
  def __init__(
      self,
      state_dict_path: str,
      model_class: Callable[..., torch.nn.Module],
      model_params: Dict[str, Any],
      device: str = 'CPU'):
    """Implementation of the ModelHandler interface for PyTorch.

    Example Usage::

      pcoll | RunInference(
      PytorchModelHandlerKeyedTensor(state_dict_path="my_uri"))

    **NOTE:** This API and its implementation are under development and
    do not provide backward compatibility guarantees.

    See https://pytorch.org/tutorials/beginner/saving_loading_models.html
    for details

    Args:
      state_dict_path: path to the saved dictionary of the model state.
      model_class: class of the Pytorch model that defines the model
        structure.
      device: the device on which you wish to run the model. If
        ``device = GPU`` then a GPU device will be used if it is available.
        Otherwise, it will be CPU.
    """
    self._state_dict_path = state_dict_path
    if device == 'GPU' and torch.cuda.is_available():
      self._device = torch.device('cuda')
    else:
      self._device = torch.device('cpu')
    self._model_class = model_class
    self._model_params = model_params

  def load_model(self) -> torch.nn.Module:
    """Loads and initializes a Pytorch model for processing."""
    return _load_model(
        self._model_class,
        self._state_dict_path,
        self._device,
        **self._model_params)

  def run_inference(
      self,
      batch: Sequence[Dict[str, torch.Tensor]],
      model: torch.nn.Module,
      inference_args: Optional[Dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """
    Runs inferences on a batch of Keyed Tensors and returns an Iterable of
    Tensor Predictions.

    For the same key across all examples, this will stack all Tensors values
    in a vectorized format to optimize the inference call.

    Args:
      batch: A sequence of keyed Tensors. These Tensors should be batchable,
        as this method will call `torch.stack()` and pass in batched Tensors
        with dimensions (batch_size, n_features, etc.) into the model's
        forward() function.
      model: A PyTorch model.
      inference_args: Non-batchable arguments required as inputs to the model's
        forward() function. Unlike Tensors in `batch`, these parameters will
        not be dynamically batched

    Returns:
      An Iterable of type PredictionResult.
    """
    inference_args = {} if not inference_args else inference_args

    # If elements in `batch` are provided as a dictionaries from key to Tensors,
    # then iterate through the batch list, and group Tensors to the same key
    key_to_tensor_list = defaultdict(list)
    for example in batch:
      for key, tensor in example.items():
        key_to_tensor_list[key].append(tensor)
    key_to_batched_tensors = {}
    for key in key_to_tensor_list:
      batched_tensors = torch.stack(key_to_tensor_list[key])
      batched_tensors = _convert_to_device(batched_tensors, self._device)
      key_to_batched_tensors[key] = batched_tensors
    predictions = model(**key_to_batched_tensors, **inference_args)
    return [PredictionResult(x, y) for x, y in zip(batch, predictions)]

  def get_num_bytes(self, batch: Sequence[torch.Tensor]) -> int:
    """
    Returns:
       The number of bytes of data for a batch of Dict of Tensors.
    """
    # If elements in `batch` are provided as a dictionaries from key to Tensors
    return sum(
        (el.element_size() for tensor in batch for el in tensor.values()))

  def get_metrics_namespace(self) -> str:
    """
    Returns:
       A namespace for metrics collected by the RunInference transform.
    """
    return 'RunInferencePytorch'
