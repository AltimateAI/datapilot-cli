#
#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

from __future__ import annotations

from typing import Any
from typing import Optional

from pydantic import ConfigDict

from vendor.dbt_artifacts_parser.parsers.base import BaseParserModel


class SemanticManifestV1(BaseParserModel):
    """
    Semantic Manifest artifact produced by dbt when parsing a project with semantic layer definitions.

    The semantic manifest contains:
    - semantic_models: Starting points of data with entities, dimensions, and measures
    - metrics: Functions combining measures, constraints to define quantitative indicators
    - project_configuration: Project-level settings including time spine configurations
    - saved_queries: Pre-built queries for common MetricFlow use cases
    """

    model_config = ConfigDict(extra="allow")

    semantic_models: Optional[list[dict[str, Any]]] = None
    metrics: Optional[list[dict[str, Any]]] = None
    project_configuration: Optional[dict[str, Any]] = None
    saved_queries: Optional[list[dict[str, Any]]] = None
