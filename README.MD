# DICOM De-identification Pipeline with Pixel Redaction

This repository provides a robust Python-based pipeline for **DICOM anonymization**, focusing on **pixel-level redaction** using OCR. It supports:

- Grayscale and RGB DICOM images
- Single-frame and multi-frame (cine loop) DICOMs
- EasyOCR-based detection and removal of PHI
- Preservation of image scale and data fidelity

## 📁 Directory Structure

- de_id_config.yaml Which is the config.yaml that takes in the diectory that you are uploading, the manifest which has the original accessionn_num and then subject_id which will be the new number, the directory where you want it output. For example:


## NOTE pip install easyocr first
---

## ⚙️ Configuration

The `config.yaml` file controls pipeline parameters:

```yaml
input_dir: "/mnt/isilon/dbhi_external_imaging/DeID/Anon70558"
output_base_dir: "/mnt/isilon/dbhi_external_imaging/DeID/DeID_Study/"
csv_output_manifest: "/mnt/isilon/dbhi_external_imaging/DeID/anonymization_log.csv"
manifest_path: "/mnt/isilon/dbhi_external_imaging/DeID/ultrasoundtest.csv"
redaction_mode: "Smart"

🧠 OCR and Redaction Logic
Uses EasyOCR for text detection

Converts grayscale to RGB for OCR, but applies redaction on original pixel scale

Applies a binary mask to zero out detected text regions

Handles both 8-bit and 16-bit DICOMs

Supports multi-frame decompression and safe redaction per frame



