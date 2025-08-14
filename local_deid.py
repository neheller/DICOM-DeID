import os
import hashlib
import pydicom
import pandas as pd
from tqdm import tqdm
import yaml
from datetime import datetime
from pydicom.uid import generate_uid
from pydicom.dataset import FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian
from pydicom.filewriter import dcmwrite
from pixel_deid import DicomPixelRedactor
from pydicom.misc import is_dicom


# --- Keep Tags (normalized names) ---
KEEP_TAGS = set([
    'AcquisitionMatrix', 'AngioFlag', 'BurnedInAnnotation', 'CodeMeaning', 'CodeValue',
    'CodingSchemeDesignator', 'dBdt', 'DeidentificationMethodCodeSequence', 'DeviceSerialNumber',
    'EchoNumbers', 'EchoTime', 'EchoTrainLength', 'FlipAngle', 'FrameOfReferenceUID',
    'ImageOrientation', 'ViewName', 'ImageOrientationPatient', 'ImageOrientationSlide',
    'ImagePosition', 'ImagePositionPatient', 'ImageType', 'ImagingFrequency',
    'InPlanePhaseEncodingDirection', 'InstanceNumber', 'InstitutionAddress',
    'InstitutionName', 'InversionTime', 'Laterality', 'LongCodeValue',
    'MagneticFieldStrength', 'Manufacturer', 'ManufacturerModelName', 'Modality',
    'MRAcquisitionType', 'NumberOfAverages', 'NumberOfPhaseEncodingSteps', 'NumberOfSlices',
    'NumberOfTimeSlices', 'PatientPosition', 'PercentPhaseFieldOfView', 'PercentSampling',
    'PerformedProcedureStepDescription', 'PixelBandwidth', 'PixelSpacing',
    'PositionReferenceIndicator', 'ProtocolName', 'RepetitionTime', 'RequestedProcedureDescription',
    'SAR', 'ScanningSequence', 'ScanOptions', 'SequenceName', 'SequenceVariant',
    'SeriesDescription', 'SeriesNumber', 'SliceLocation', 'SliceThickness', 'SoftwareVersions',
    'SOPClassUID', 'SourceOrientation', 'SourcePosition', 'SpacingBetweenSlices',
    'StationName', 'StudyDescription', 'URNCodeValue', 'VariableFlipAngleFlag','Specific Character Set', 
    'SOP Instance UID', "Manufacturer's Model Name", 'Anatomic Region',
    'Sequence',   'Body Part Examined', 'Imager Pixel Spacing', 'Relative X-Ray Exposure', 'Exposure Index', 
    'Target Exposure Index', 'Deviation Index', 'Detector Type', 'Detector Configuration',   'Patient Orientation', 
    'Image Laterality', 'Samples per Pixel', 'Photometric Interpretation', 'Rows', 'Columns', 'Bits Allocated', 
    'Bits Stored', 'High Bit', 'Pixel Representation', 'Longitudinal Temporal Information Modified',
    'Pixel Intensity Relationship', 'Pixel Intensity Relationship Sign', 'Window Center', 'Window Width', 
    'Rescale Intercept', 'Rescale Slope', 'Rescale Type', 'Window Center & Width Explanation', 
    'Lossy Image Compression', 'Acquisition Context Sequence', 'Filler Order Number / Imaging Service Request', 
    'Presentation LUT Shape', 'Pixel Data','Planar Configuration', 'planarconfiguration','pixelaspectratio','numberofframes',
    'referencedperformedprocedurestepsequence','performedprotocolcodesequence','requestattributessequence','procedurecodesequence',
    'anatomicregionsequence','acquisitioncontextsequence','sequenceofultrasoundregions','ultrasoundcolordatapresent',
    'transducerdata'
])

REPLACEMENT_TAGS = {
    'AccessionNumber', 'PatientID', 'PatientName', 'StudyID', 'PatientIdentityRemoved','StudyInstanceUID','SeriesInstanceUID',
    'SOPInstanceUID','MediaStorageSOPInstanceUID'
}



def process_file(
    root, file, accession_map, accession_uid_map, folder_uid_map,
    output_base_dir, output_manifest, uid_gen, input_dir, normalize_tag,
    redactor
):
    full_path = os.path.join(root, file)

    if not is_dicom(full_path):
        print(f"⚠️ Skipping non-DICOM file: {full_path}")
        return
    try:
        ds = pydicom.dcmread(full_path)
    except Exception as e:
        print(f"❌ Failed to read: {full_path} — {e}")
        return
    accession = str(ds.get("AccessionNumber", "")).strip()
    print(accession)
    if accession not in accession_map:
        print(accession, "notthere")
        return

    deid_acc = accession_map[accession]
    try:
        ds.decompress()
        if not hasattr(ds, "PixelData") or not ds.PixelData:
            raise ValueError("PixelData is missing after decompression")
        _ = ds.pixel_array  # Force actual decode
        print(f"decompressed{full_path}")
    except Exception as e:
        print(f"❌ Failed to decompress {full_path}")
        print(f"   TransferSyntaxUID: {ds.file_meta.TransferSyntaxUID}")
        print(f"   Rows: {getattr(ds, 'Rows', 'Unknown')} | Columns: {getattr(ds, 'Columns', 'Unknown')} | BitsAllocated: {getattr(ds, 'BitsAllocated', 'Unknown')}")
        print(f"   SamplesPerPixel: {getattr(ds, 'SamplesPerPixel', 'Unknown')} | NumberOfFrames: {getattr(ds, 'NumberOfFrames', 1)}")
        print(f"   PixelRepresentation: {getattr(ds, 'PixelRepresentation', 'Unknown')}")
        print(f"   PhotometricInterpretation: {getattr(ds, 'PhotometricInterpretation', 'Unknown')}")
        print(f"   Error: {e}")
        return
        

    # Cache consistent UIDs
    if deid_acc not in accession_uid_map:
        accession_uid_map[deid_acc] = {
            "study_uid": uid_gen.generate(),
            "series_uid": uid_gen.generate()
        }


    rel_path = os.path.relpath(full_path, input_dir)
    path_parts = rel_path.split(os.sep)[:-1]

    # Replace first folder in path with deid accession number
    deid_parts = [deid_acc]  # first folder is deid accession
    for part in path_parts[1:]:  # keep remaining parts with UUIDs
        if part not in folder_uid_map:
            folder_uid_map[part] = uid_gen.generate()
        deid_parts.append(folder_uid_map[part])  

    sop_uid = uid_gen.generate()
    new_filename = f"{deid_acc}_{sop_uid}.dcm"
    deid_parts.append(new_filename)

    output_path = os.path.join(output_base_dir, *deid_parts)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Replace identifying tags
    ds.AccessionNumber = deid_acc
    ds.PatientID = deid_acc
    ds.PatientName = deid_acc
    ds.StudyID = deid_acc
    ds.PatientIdentityRemoved = "YES"
    ds.StudyInstanceUID = accession_uid_map[deid_acc]["study_uid"]
    ds.SeriesInstanceUID = accession_uid_map[deid_acc]["series_uid"]
    ds.SOPInstanceUID = sop_uid

    # --- Add Required File Meta Info ---
    file_meta = FileMetaDataset()
    file_meta.MediaStorageSOPClassUID = sop_uid
    file_meta.MediaStorageSOPInstanceUID = sop_uid
    file_meta.ImplementationClassUID = "1.3.6.1.4.1.11129.5.1"
    file_meta.TransferSyntaxUID = pydicom.uid.ExplicitVRLittleEndian

    ds.file_meta = file_meta
    ds.is_little_endian = True
    ds.is_implicit_VR = False

    # Prepare for tag filtering
    keep_normalized = {normalize_tag(t) for t in KEEP_TAGS}
    kept_tags = []
    wiped_tags = []
    #####

    ###
    for elem in list(ds.iterall()):
        norm = normalize_tag(elem.name)
        if norm in keep_normalized:
            kept_tags.append(elem.name)
            continue

        if normalize_tag(elem.name) in {normalize_tag(t) for t in REPLACEMENT_TAGS}:
            continue

        try:
            if elem.tag.is_private:
                del ds[elem.tag]
            else:
                try:
                    ds[elem.tag].value = ''

                except Exception:
                    del ds[elem.tag]
            wiped_tags.append(elem.name)
        except Exception as e:
            #print('new scrubbing error', e)
            continue


    ds.is_little_endian = True
    ds.is_implicit_VR = False
    ds.file_meta.TransferSyntaxUID = ExplicitVRLittleEndian

    #dcmwrite(output_path, ds, write_like_original=False)

    
    ###ds.save_as(output_path)  ### old
    # Scrubbing and tag editing...
    print("about to redact")
    redactor.redact(ds, output_path)


    output_manifest.append({
        "original_path": full_path,
        "deid_path": output_path,
        "original_accession": accession,
        "deid_accession": deid_acc,
        "study_uid": ds.StudyInstanceUID,
        "series_uid": ds.SeriesInstanceUID,
        "sop_instance_uid": sop_uid,
        "AccessionNumber": ds.AccessionNumber,
        "PatientIdentityRemoved": ds.PatientIdentityRemoved,
        "PatientName": ds.PatientName,
        "StudyID": ds.StudyID,
        "PatientID": ds.PatientID
    })

    print(f"\n📄 {full_path} → {output_path}")
    print(f"   ✅ Kept Tags: {kept_tags}")
    print(f"   ❌ Wiped Tags: {wiped_tags}")



def main():
    with open("de_id_config.yaml", "r") as f:
        config = yaml.safe_load(f)

    # --- Configuration ---

    input_dir = config["input_dir"]
    output_base_dir = config["output_base_dir"]
    csv_output_manifest = config["csv_output_manifest"]
    manifest_path = config["manifest_path"]

    redaction = config["redaction_mode"] 

    # --- UID generator class ---
    class UIDGenerator:
        def __init__(self, org_root="1.3.6.1.4.1.11129.5.1"):
            self.org_root = org_root
            self.current_uid = None

        def generate(self):
            while True:
                n = datetime.now()
                new_uid = f"{self.org_root}.{n.year}{n.month}{n.day}{n.minute}{n.second}{n.microsecond}"
                if new_uid != self.current_uid:
                    self.current_uid = new_uid
                    return new_uid

    uid_gen = UIDGenerator()

    # --- Normalizer ---
    def normalize_tag(tag_name):
        return tag_name.replace(" ", "").lower()

    # --- Load Manifest ---
    manifest = pd.read_csv(manifest_path, encoding='ISO-8859-1')
    accession_map = dict(zip(
        manifest['accession_num'].astype(str),
        manifest['subject_id'].astype(str)
    ))

    # --- UID mapping for accessions and folder names ---
    accession_uid_map = {}
    folder_uid_map = {}

    # --- Output tracker ---
    output_manifest = []
    redactor = DicomPixelRedactor(redaction_mode=redaction)

    # --- Process Files ---
    for root, _, files in os.walk(input_dir):
        for file in tqdm(files):
            try:
                process_file(
                    root, file, accession_map, accession_uid_map, folder_uid_map,
                    output_base_dir, output_manifest, uid_gen, input_dir, normalize_tag,
                    redactor
                )
            except Exception as e:
                print(f"❌ Error processing {file}: {e}")

    # --- Save Output CSV Manifest ---
    pd.DataFrame(output_manifest).to_csv(csv_output_manifest, index=False)
    print(f"\n✅ Complete. Manifest written to: {csv_output_manifest}")
    
    
if __name__ == "__main__":
    main()

