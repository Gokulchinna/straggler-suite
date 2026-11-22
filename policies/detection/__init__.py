from policies.detection.base import StragglerDetector
from policies.detection.spark_median import SparkMedianDetector
from policies.detection.capacity_normalized import CapacityNormalizedDetector

__all__ = ["StragglerDetector", "SparkMedianDetector", "CapacityNormalizedDetector"]
