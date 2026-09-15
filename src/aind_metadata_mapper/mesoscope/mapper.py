"""Mesoscope mapper module."""

import json
import re
from datetime import datetime, tzinfo
from math import isfinite
from typing import Any, Optional
from zoneinfo import ZoneInfo

from aind_data_schema.components.configs import (
    Channel,
    CoupledPlane,
    DetectorConfig,
    ImagingConfig,
    LaserConfig,
    PlanarImage,
    Plane,
    SamplingStrategy,
    TriggerType,
)
from aind_data_schema.components.coordinates import CoordinateSystemLibrary, Scale, Translation
from aind_data_schema.components.identifiers import Code, Software
from aind_data_schema.core.acquisition import (
    Acquisition,
    AcquisitionSubjectDetails,
    DataStream,
    PerformanceMetrics,
    StimulusEpoch,
)
from aind_data_schema_models.brain_atlas import CCFv3
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.stimulus_modality import StimulusModality
from aind_data_schema_models.units import PowerUnit, SizeUnit, VolumeUnit
from aind_metadata_extractor.models.mesoscope import MesoscopeExtractModel

from aind_metadata_mapper.base import MapperJob, MapperJobSettings

PACIFIC_TIMEZONE = ZoneInfo("America/Los_Angeles")
DEFAULT_FOV_COORDINATE_UM = 1.5
DEFAULT_PIXEL_SIZE_UM = 0.78
DEFAULT_LASER_WAVELENGTH_NM = 920
DEFAULT_CHANNEL_NAME = "Ophys Channel"
DEFAULT_DETECTOR_NAME = "Unknown Detector"
DEFAULT_LIGHT_SOURCE_NAME = "Laser"
DEFAULT_IMAGE_DEVICE_NAME = "Mesoscope"
DEFAULT_EXPOSURE_TIME_MS = 1.0
CAMERA_ASSEMBLY_ORDER = {"Behavior": 0, "Eye": 1, "Face": 2}
DEFAULT_VISUAL_STIMULUS_MODALITIES = [StimulusModality.VISUAL]
DEFAULT_VISUAL_STIMULUS_TYPE = "Visual Stimulation"
LEGACY_TARGETED_STRUCTURE_LOOKUP = {
    385: CCFv3.VISP,
    394: CCFv3.VISAM,
    402: CCFv3.VISAL,
    409: CCFv3.VISL,
    417: CCFv3.VISRL,
    533: CCFv3.VISPM,
    312782574: CCFv3.VISLI,
}


class MesoscopeMapper(MapperJob):
    """Transform extracted mesoscope metadata into Acquisition metadata."""

    def _validate_metadata(self, metadata: dict) -> MesoscopeExtractModel:
        """Validate extracted metadata against MesoscopeExtractModel."""
        return MesoscopeExtractModel.model_validate(metadata)

    def _get_required_value(self, container: dict, key: str, context: str) -> Any:
        """Return a required value from a dictionary."""
        value = container.get(key)
        if value in (None, ""):
            raise ValueError(f"Mesoscope {context}.{key} is required.")
        return value

    def _parse_datetime(
        self,
        value: Any,
        field_name: str,
        fallback_tz: Optional[tzinfo] = None,
    ) -> datetime:
        """Parse a datetime and attach Pacific time to naive values."""
        if value is None:
            raise ValueError(f"{field_name} is required.")

        if isinstance(value, datetime):
            parsed = value
        elif isinstance(value, str):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        else:
            raise TypeError(f"{field_name} must be an ISO 8601 string or datetime.")

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=fallback_tz or PACIFIC_TIMEZONE)
        return parsed

    def _parse_float(self, value: Any, field_name: str) -> float:
        """Parse a required numeric field."""
        if value in (None, ""):
            raise ValueError(f"{field_name} is required.")
        return float(value)

    def _normalize_camera_base_name(self, name: str) -> str:
        """Normalize a camera name to its shared base."""
        normalized = re.sub(r"\s+camera\s+assembly\s*$", "", name, flags=re.IGNORECASE).strip()
        normalized = re.sub(r"\s+camera\s*$", "", normalized, flags=re.IGNORECASE).strip()
        return normalized

    def _normalize_camera_assembly_name(self, name: str) -> str:
        """Normalize a camera name to upgrader-compatible assembly naming."""
        base = self._normalize_camera_base_name(name)
        if not base:
            raise ValueError("Camera name cannot be empty.")
        return f"{base} camera assembly"

    def _extract_camera_label(self, session_key: str, camera_metadata: dict) -> Optional[str]:
        """Extract a camera label from extractor session metadata."""
        report = camera_metadata.get("RecordingReport", {})
        label = report.get("CameraLabel")
        if isinstance(label, str) and label.strip():
            return label.strip()

        for candidate in CAMERA_ASSEMBLY_ORDER:
            if candidate.lower() in session_key.lower():
                return candidate
        return None

    def _collect_camera_assembly_names(self, session_metadata: dict) -> list[str]:
        """Collect active camera assemblies from session metadata."""
        assemblies = []
        for session_key, camera_metadata in session_metadata.items():
            if session_key == "platform" or not isinstance(camera_metadata, dict):
                continue
            label = self._extract_camera_label(session_key, camera_metadata)
            if label is None:
                continue
            assemblies.append(self._normalize_camera_assembly_name(label))

        unique_assemblies = []
        for name in sorted(
            assemblies, key=lambda item: CAMERA_ASSEMBLY_ORDER.get(self._normalize_camera_base_name(item), 99)
        ):
            if name not in unique_assemblies:
                unique_assemblies.append(name)
        return unique_assemblies

    def _resolve_subject_id(self, platform: dict, job_settings: dict) -> str:
        """Resolve and validate subject_id."""
        platform_subject_id = platform.get("subject_id")
        job_subject_id = job_settings.get("subject_id")
        if platform_subject_id and job_subject_id and str(platform_subject_id) != str(job_subject_id):
            raise ValueError(
                "Mesoscope platform.subject_id does not match job_settings.subject_id: "
                f"{platform_subject_id} != {job_subject_id}."
            )
        subject_id = platform_subject_id or job_subject_id
        if subject_id in (None, ""):
            raise ValueError("Mesoscope subject_id is required.")
        return str(subject_id)

    def _resolve_ethics_review_id(self, job_settings: dict) -> Optional[list[str]]:
        """Normalize IACUC protocol values."""
        protocol = job_settings.get("iacuc_protocol")
        if protocol in (None, "", []):
            return None
        if isinstance(protocol, list):
            normalized = [str(item) for item in protocol if item not in (None, "")]
            return normalized or None
        return [str(protocol)]

    def _resolve_imaging_device_name(self, platform: dict, instrument_id: str) -> str:
        """Resolve imaging device name for ImagingConfig.device_name.

        Native mesoscope mapping keeps the platform/session alias here to match
        upgraded production outputs. Acquisition.instrument_id still comes from
        the explicit full instrument ID in job_settings.instrument_id.
        """
        return str(platform.get("rig_id") or instrument_id)

    def _get_targeted_structure(self, value: Any) -> dict:
        """Map a legacy targeted structure value to a CCFv3 structure."""
        if isinstance(value, dict):
            return value

        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                raise ValueError("targeted_structure_id is required.")
            if stripped.isdigit():
                value = int(stripped)
            else:
                acronym = stripped.upper()
                if hasattr(CCFv3, acronym):
                    return getattr(CCFv3, acronym).model_dump()
                raise ValueError(f"Unsupported targeted_structure_id: {value}")

        if isinstance(value, int):
            if value in LEGACY_TARGETED_STRUCTURE_LOOKUP:
                return LEGACY_TARGETED_STRUCTURE_LOOKUP[value].model_dump()
            raise ValueError(f"Unsupported targeted_structure_id: {value}")

        raise TypeError(f"Unsupported targeted_structure_id type: {type(value).__name__}")

    def _validate_fov_reference(self, job_settings: dict) -> None:
        """Validate that the mapper only emits Bregma-referenced coordinates."""
        reference = job_settings.get("fov_reference")
        if reference and str(reference).lower() != "bregma":
            raise ValueError(f"Unsupported fov_reference: {reference}")

    def _get_fov_translation_mm(self, job_settings: dict) -> list[float]:
        """Return FOV AP/ML translation in millimeters."""
        self._validate_fov_reference(job_settings)
        ap_um = float(job_settings.get("fov_coordinate_ap", DEFAULT_FOV_COORDINATE_UM))
        ml_um = float(job_settings.get("fov_coordinate_ml", DEFAULT_FOV_COORDINATE_UM))
        return [ap_um / 1000.0, ml_um / 1000.0, 0.0]

    def _get_image_dimensions(self, tiff_header: list) -> Scale:
        """Extract image dimensions from tiff metadata."""
        if not tiff_header or not isinstance(tiff_header[0], dict):
            raise ValueError("Mesoscope tiff_header[0] must be present.")

        header = tiff_header[0]
        width = self._parse_float(
            header.get("SI.hRoiManager.pixelsPerLine"), "tiff_header[0].SI.hRoiManager.pixelsPerLine"
        )
        height = self._parse_float(
            header.get("SI.hRoiManager.linesPerFrame"),
            "tiff_header[0].SI.hRoiManager.linesPerFrame",
        )
        return Scale(scale=[width, height])

    def _get_scale_transform(self) -> Scale:
        """Return legacy pixel scale for planar images."""
        scale_mm = DEFAULT_PIXEL_SIZE_UM / 1000.0
        return Scale(scale=[scale_mm, scale_mm])

    def _build_imaging_channel(self) -> Channel:
        """Build default mesoscope imaging channel."""
        return Channel(
            channel_name=DEFAULT_CHANNEL_NAME,
            detector=DetectorConfig(
                device_name=DEFAULT_DETECTOR_NAME,
                exposure_time=DEFAULT_EXPOSURE_TIME_MS,
                trigger_type=TriggerType.INTERNAL,
            ),
            light_sources=[
                LaserConfig(
                    device_name=DEFAULT_LIGHT_SOURCE_NAME,
                    wavelength=DEFAULT_LASER_WAVELENGTH_NM,
                    wavelength_unit=SizeUnit.NM,
                    power=None,
                    power_unit=PowerUnit.MW,
                )
            ],
        )

    def _get_plane_power(self, _group: dict, plane: dict, group_index: int, plane_index: int) -> float:
        """Resolve per-plane calculated power in milliwatts."""
        field_name = f"imaging_plane_groups[{group_index}].imaging_planes[{plane_index}].calculated_power_mw"
        value = plane.get("calculated_power_mw")
        if isinstance(value, bool):
            raise ValueError(f"{field_name} must be a finite, nonnegative number.")
        try:
            power = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field_name} must be a finite, nonnegative number.") from exc
        if not isfinite(power) or power < 0:
            raise ValueError(f"{field_name} must be a finite, nonnegative number.")
        return power

    def _build_plane(
        self, group: dict, plane: dict, group_index: int, plane_index: int, global_index: int
    ) -> Plane | CoupledPlane:
        """Build a Plane or CoupledPlane from a platform imaging plane."""
        targeted_structure = self._get_targeted_structure(plane.get("targeted_structure_id"))
        base_kwargs = dict(
            depth=self._parse_float(
                plane.get("targeted_depth"), f"imaging_plane_groups[{group_index}].planes[{plane_index}].targeted_depth"
            ),
            depth_unit=SizeUnit.UM,
            power=self._get_plane_power(group, plane, group_index, plane_index),
            power_unit=PowerUnit.MW,
            targeted_structure=targeted_structure,
        )

        if len(group["imaging_planes"]) == 1:
            return Plane(**base_kwargs)

        partner_index = global_index + 1 if plane_index == 0 else global_index - 1
        power_ratio = self._parse_float(
            group.get("scanimage_split_percent"),
            f"imaging_plane_groups[{group_index}].scanimage_split_percent",
        )
        return CoupledPlane(
            **base_kwargs,
            plane_index=global_index,
            coupled_plane_index=partner_index,
            power_ratio=power_ratio,
        )

    def _build_stream_notes(
        self, plane_index: int, scanimage_roi_index: Any, magnification: Optional[str]
    ) -> Optional[str]:
        """Build stream notes for plane metadata without dedicated schema fields."""
        extras = []
        if magnification not in (None, ""):
            extras.append(f"magnification={magnification}")
        if scanimage_roi_index is not None:
            extras.append(f"scanimage_roi_index={scanimage_roi_index}")
        if not extras:
            return None
        return f"FOV {plane_index}: " + ", ".join(extras)

    def _build_imaging_config(
        self,
        tiff_header: list,
        platform: dict,
        job_settings: dict,
        imaging_device_name: str,
    ) -> tuple[ImagingConfig, Optional[str]]:
        """Build ImagingConfig and any stream notes."""
        dimensions = self._get_image_dimensions(tiff_header)
        translation = Translation(translation=self._get_fov_translation_mm(job_settings))
        channel = self._build_imaging_channel()
        images = []
        notes = []
        global_plane_index = 0
        magnification = job_settings.get("magnification")

        groups = self._get_required_value(platform, "imaging_plane_groups", "platform")
        if not groups:
            raise ValueError("Mesoscope platform.imaging_plane_groups must contain at least one group.")
        for group_index, group in enumerate(groups):
            planes = self._get_required_value(group, "imaging_planes", f"platform.imaging_plane_groups[{group_index}]")
            if len(planes) not in (1, 2):
                raise ValueError(
                    f"Mesoscope imaging_plane_groups[{group_index}] must contain 1 or 2 "
                    f"imaging_planes, got {len(planes)}."
                )

            for plane_index, plane in enumerate(planes):
                schema_plane = self._build_plane(group, plane, group_index, plane_index, global_plane_index)
                images.append(
                    PlanarImage(
                        channel_name=channel.channel_name,
                        planes=[schema_plane],
                        image_to_acquisition_transform=[self._get_scale_transform(), translation],
                        dimensions=dimensions,
                        dimensions_unit=SizeUnit.PX,
                    )
                )
                note = self._build_stream_notes(global_plane_index, plane.get("scanimage_roi_index"), magnification)
                if note is not None:
                    notes.append(note)
                global_plane_index += 1

        imaging_config = ImagingConfig(
            device_name=imaging_device_name,
            channels=[channel],
            images=images,
            sampling_strategy=SamplingStrategy(
                frame_rate=self._parse_float(
                    groups[0].get("acquisition_framerate_Hz"),
                    "imaging_plane_groups[0].acquisition_framerate_Hz",
                )
            ),
        )
        return imaging_config, "; ".join(notes) if notes else None

    def _build_performance_metrics(self, epoch: dict) -> Optional[PerformanceMetrics]:
        """Build performance metrics when present."""
        reward_value = epoch.get("reward_consumed_during_epoch")
        reward_unit = epoch.get("reward_consumed_unit")
        if reward_value not in (None, "") and reward_unit is None:
            reward_unit = VolumeUnit.ML if float(reward_value) < 3 else VolumeUnit.UL

        if not any(
            [
                epoch.get("trials_total"),
                epoch.get("trials_finished"),
                epoch.get("trials_rewarded"),
                reward_value,
            ]
        ):
            return None

        return PerformanceMetrics(
            trials_total=epoch.get("trials_total"),
            trials_finished=epoch.get("trials_finished"),
            trials_rewarded=epoch.get("trials_rewarded"),
            reward_consumed_during_epoch=reward_value,
            reward_consumed_unit=reward_unit,
            output_parameters=epoch.get("output_parameters"),
        )

    def _get_stimulus_parameters(self, stimulus_parameters: Any) -> Optional[dict]:
        """Normalize the extracted stimulus parameter structure."""
        parameters = None
        if isinstance(stimulus_parameters, list):
            if len(stimulus_parameters) == 1:
                if not isinstance(stimulus_parameters[0], dict):
                    raise TypeError("stimulus_parameters entries must be dictionaries.")
                parameters = self._normalize_visual_stimulation_parameters(stimulus_parameters[0])
            else:
                parameters = {}
                for index, item in enumerate(stimulus_parameters):
                    if not isinstance(item, dict):
                        raise TypeError("stimulus_parameters entries must be dictionaries.")
                    key = item.get("stimulus_name") or f"Stimulus_{index}"
                    if key in parameters:
                        key = f"{key}_{index}"
                    parameters[key] = self._normalize_visual_stimulation_parameters(item)
        elif stimulus_parameters is not None:
            if not isinstance(stimulus_parameters, dict):
                raise TypeError("stimulus_parameters must be a dictionary or list of dictionaries.")
            parameters = stimulus_parameters
        return parameters

    def _build_code(self, epoch: dict) -> Optional[Code]:
        """Build a Code object from extracted Camstim epoch data."""
        script = epoch.get("script") or {}
        parameters = self._get_stimulus_parameters(epoch.get("stimulus_parameters"))
        software = epoch.get("software") or []
        core_dependency = None
        if software:
            first_software = software[0]
            core_dependency = Software(
                name=first_software.get("name", "unknown"),
                version=first_software.get("version"),
            )

        if script.get("parameters"):
            if parameters is None:
                parameters = script["parameters"]
            else:
                parameters = parameters | script["parameters"]

        if not script and parameters is None and core_dependency is None:
            return None

        if not script:
            return Code(
                name="Code",
                version="unknown",
                url="unknown",
                parameters=parameters,
                core_dependency=core_dependency,
            )

        return Code(
            name=script.get("name", "Unknown Script"),
            version=script.get("version", "unknown"),
            url=script.get("url", "") or "",
            parameters=parameters,
            core_dependency=core_dependency,
        )

    def _resolve_stimulus_modalities(self, epoch: dict) -> list[StimulusModality]:
        """Resolve stimulus modalities from extractor output."""
        modalities = epoch.get("stimulus_modalities")
        if modalities:
            return modalities
        return DEFAULT_VISUAL_STIMULUS_MODALITIES

    def _normalize_visual_stimulation_parameters(self, parameters: dict[str, Any]) -> dict[str, Any]:
        """Add legacy visual-stimulation defaults without overwriting supplied values."""
        normalized = dict(parameters)
        normalized.setdefault("stimulus_type", DEFAULT_VISUAL_STIMULUS_TYPE)
        normalized.setdefault("notes", None)
        return normalized

    def _build_stimulus_epoch(self, epoch: dict, fallback_tz: tzinfo) -> StimulusEpoch:
        """Build a StimulusEpoch from extracted Camstim epoch data."""
        task_parameters = (epoch.get("output_parameters") or {}).get("task_parameters") or {}
        stimulus_start = self._parse_datetime(epoch.get("stimulus_start_time"), "stimulus_start_time", fallback_tz)
        stimulus_end = self._parse_datetime(epoch.get("stimulus_end_time"), "stimulus_end_time", fallback_tz)
        if stimulus_end < stimulus_start:
            raise ValueError("stimulus_end_time must be greater than or equal to stimulus_start_time.")

        return StimulusEpoch(
            stimulus_start_time=stimulus_start,
            stimulus_end_time=stimulus_end,
            stimulus_name=self._get_required_value(epoch, "stimulus_name", "camstim_epchs"),
            stimulus_modalities=self._resolve_stimulus_modalities(epoch),
            performance_metrics=self._build_performance_metrics(epoch),
            code=self._build_code(epoch),
            active_devices=epoch.get("stimulus_device_names") or [],
            notes=epoch.get("notes"),
            curriculum_status=task_parameters.get("stage_in_use"),
        )

    def _get_acquisition_bounds(
        self,
        stream_start: datetime,
        stream_end: datetime,
        stimulus_epochs: list[StimulusEpoch],
    ) -> tuple[datetime, datetime]:
        """Return acquisition bounds spanning stream and stimulus intervals."""
        acquisition_start = stream_start
        acquisition_end = stream_end

        for epoch in stimulus_epochs:
            acquisition_start = min(acquisition_start, epoch.stimulus_start_time)
            acquisition_end = max(acquisition_end, epoch.stimulus_end_time)

        return acquisition_start, acquisition_end

    def transform(self, metadata: dict) -> Acquisition:
        """Transform extracted mesoscope metadata into Acquisition."""
        metadata_model = self._validate_metadata(metadata)
        raw_metadata = metadata_model.model_dump()

        tiff_header = self._get_required_value(raw_metadata, "tiff_header", "metadata")
        session_metadata = self._get_required_value(raw_metadata, "session_metadata", "metadata")
        platform = self._get_required_value(session_metadata, "platform", "session_metadata")
        job_settings = self._get_required_value(raw_metadata, "job_settings", "metadata")
        instrument_id = str(self._get_required_value(job_settings, "instrument_id", "job_settings"))
        acquisition_type = raw_metadata.get("camstim_session_type") or platform.get("stimulus_name")
        if acquisition_type in (None, ""):
            raise ValueError("Mesoscope camstim_session_type is required.")

        subject_id = self._resolve_subject_id(platform, job_settings)
        session_start = self._parse_datetime(
            self._get_required_value(job_settings, "session_start_time", "job_settings"),
            "session_start_time",
        )
        session_end = self._parse_datetime(
            self._get_required_value(job_settings, "session_end_time", "job_settings"),
            "session_end_time",
            session_start.tzinfo,
        )
        if session_end < session_start:
            raise ValueError("session_end_time must be greater than or equal to session_start_time.")

        imaging_device_name = self._resolve_imaging_device_name(platform, instrument_id)
        imaging_config, stream_notes = self._build_imaging_config(
            tiff_header, platform, job_settings, imaging_device_name
        )

        active_devices = [imaging_device_name, DEFAULT_IMAGE_DEVICE_NAME]
        active_devices.extend(self._collect_camera_assembly_names(session_metadata))

        data_stream = DataStream(
            stream_start_time=session_start,
            stream_end_time=session_end,
            modalities=[Modality.POPHYS],
            active_devices=list(dict.fromkeys(active_devices)),
            configurations=[imaging_config],
            connections=[],
            notes=stream_notes,
        )

        stimulus_epochs = [
            self._build_stimulus_epoch(epoch, session_start.tzinfo or PACIFIC_TIMEZONE)
            for epoch in raw_metadata.get("camstim_epchs", [])
        ]
        acquisition_start, acquisition_end = self._get_acquisition_bounds(session_start, session_end, stimulus_epochs)

        subject_details = None
        if job_settings.get("mouse_platform_name"):
            subject_details = AcquisitionSubjectDetails(
                mouse_platform_name=str(job_settings["mouse_platform_name"]),
                reward_consumed_unit=VolumeUnit.ML,
            )

        experimenters = job_settings.get("experimenter_full_name", [])
        if isinstance(experimenters, str):
            experimenters = [experimenters]

        return Acquisition(
            subject_id=subject_id,
            acquisition_start_time=acquisition_start,
            acquisition_end_time=acquisition_end,
            experimenters=[name for name in experimenters if name],
            ethics_review_id=self._resolve_ethics_review_id(job_settings),
            instrument_id=instrument_id,
            acquisition_type=str(acquisition_type),
            # aind-data-schema 2.7.1 does not expose global_coordinate_system.
            # Use coordinate_system to preserve the declared schema floor.
            coordinate_system=CoordinateSystemLibrary.BREGMA_ARI,
            data_streams=[data_stream],
            stimulus_epochs=stimulus_epochs,
            subject_details=subject_details,
        )

    def run_job(self, job_settings: MapperJobSettings) -> None:
        """Run mesoscope mapper and write acquisition metadata."""
        with open(job_settings.input_filepath, "r", encoding="utf-8") as input_stream:
            metadata = json.load(input_stream)

        acquisition = self.transform(metadata)
        acquisition.write_standard_file(
            output_directory=job_settings.output_directory,
            filename_suffix=job_settings.output_filename_suffix,
        )
