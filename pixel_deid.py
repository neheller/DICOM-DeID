import numpy as np
import cv2
import easyocr
import pydicom
from pydicom.uid import ExplicitVRLittleEndian
from pydicom.filewriter import dcmwrite

class DicomPixelRedactor:
    def __init__(self, redaction_mode="Full"):
        self.redaction_mode = redaction_mode
        self.reader = easyocr.Reader(['en'], gpu=True)
        self.keywords = [
            'right', 'left', 'rt', 'lt', 'rk', 'lk', 'kidney', 'bladder',
            'sagittal', 'sag', 'transverse', 'trans', 'prone'
        ]

    def redact(self, ds, output_path):
        print("🕵️ Starting redaction")

        if "PixelData" not in ds:
            print(f"⚠️ Skipping (no pixel data): {output_path}")
            return

        try:
            # Access decoded pixel array safely
            original_array = ds.pixel_array
        except Exception as e:
            print(f"❌ Failed to access pixel array: {e}")
            return

        shape = original_array.shape
        bits = ds.BitsAllocated
        samples = int(getattr(ds, 'SamplesPerPixel', 1))
        frames = int(getattr(ds, 'NumberOfFrames', 1)) if hasattr(ds, 'NumberOfFrames') else (shape[0] if len(shape) == 4 or len(shape) == 3 and samples == 1 else 1)

        print(f"🔍 Shape: {shape}, Bits: {bits}, SamplesPerPixel: {samples}, Frames: {frames}")

        try:
            # Handle multi-frame vs single-frame
            if frames > 1:
                redacted_array = np.stack([
                    self.redact_frame(original_array[i], samples)
                    for i in range(frames)
                ])
            else:
                redacted_array = self.redact_frame(original_array, samples)

            # Convert back to original dtype and update PixelData
            ds.PixelData = redacted_array.astype(original_array.dtype).tobytes()

            # Ensure proper metadata
            ds.BitsAllocated = bits
            ds.BitsStored = bits
            ds.HighBit = bits - 1
            ds.PixelRepresentation = 0
            ds.file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
            ds.is_little_endian = True
            ds.is_implicit_VR = False

            dcmwrite(output_path, ds, write_like_original=False)
            print(f"✅ Redacted DICOM saved to: {output_path}")
        except Exception as e:
            print(f"❌ Failed to save redacted DICOM: {e}")

    def redact_frame(self, img, samples):
        try:
            rows, cols = img.shape[:2]

            # For OCR: normalize to 8-bit RGB
            if samples == 1:  # grayscale
                img_norm = cv2.normalize(img.astype(np.float32), None, 0, 255, cv2.NORM_MINMAX)
                img_uint8 = np.uint8(img_norm)
                img_rgb = cv2.cvtColor(img_uint8, cv2.COLOR_GRAY2RGB)
            else:  # color
                img_rgb = img.astype(np.uint8)

            # OCR
            results = self.reader.readtext(img_rgb)
            mask = np.ones((rows, cols), dtype=np.uint8) * 255

            for (bbox, text, prob) in results:
                if prob > 0.6:
                    cleaned_text = text.strip().lower()

                    if self.redaction_mode != "Full":
                        if any(k in cleaned_text for k in self.keywords):
                            continue
                        if len(cleaned_text) == 1 and cleaned_text.isalpha():
                            continue

                    (tl, tr, br, bl) = bbox
                    tl = (max(0, min(int(tl[0]), cols - 1)), max(0, min(int(tl[1]), rows - 1)))
                    br = (max(0, min(int(br[0]), cols - 1)), max(0, min(int(br[1]), rows - 1)))
                    cv2.rectangle(mask, tl, br, 0, thickness=-1)

            # Apply mask to original pixel array (preserving scale)
            if samples == 1:
                return np.where(mask == 0, 0, img)
            else:
                mask_rgb = np.stack([mask]*3, axis=-1)
                return np.where(mask_rgb == 0, 0, img)

        except Exception as e:
            print(f"❌ Frame redaction error: {e}")
            return img  # Fallback: return unredacted frame
