# HW № 1. Karpov D.V. student.

# CMD launch of pipeline
#   python -m luigi_pipeline CreateDataSet --data-name GSE68849 --local-scheduler
# Input args:
#  - data_name - dataset name (string). Default name is GSE68849
#  - dest_folder - name of destination folder (string). Optional parameter
#  - prep_list  - list of files to preprocessing (list). Optional parameter
#  - cols_fordrop - columns to drop (list). Optional


import luigi
from luigi.format import Nop
import io
import os
import requests
import pandas as pd
import tarfile
import gzip
from pathlib import Path


class DownloadData(luigi.Task):
    """
    Download datafile by reference.
    Dataset name taken from user input in cmd
    Link: https://www.ncbi.nlm.nih.gov/geo/download/?acc={data_name}&format=file
    """

    data_name = luigi.Parameter()
    dest_folder = luigi.Parameter()

    def output(self):
        # format=Nop because tar file is binary
        return luigi.LocalTarget(
            os.path.join(self.dest_folder, f"{self.data_name}.tar"),
            format=Nop,
        )

    def run(self):
        response = requests.get(
            f"https://www.ncbi.nlm.nih.gov/geo/download/?acc={self.data_name}&format=file",
            stream=True,
        )

        # HTTP status check
        if response.status_code == 200:
            # create destination folder
            os.makedirs(self.dest_folder, exist_ok=True)

            # save file from internet
            with self.output().open("w") as f:
                f.write(response.content)
        else:
            raise ValueError(f"Error of dataset downloading: {response.status_code}")


class ExtractFiles(luigi.Task):
    """
    Unzip downloaded file recursively 
    Get separate tables from downloaded files and save them
    """

    data_name = luigi.Parameter()
    dest_folder = luigi.Parameter()

    # chain with previous step in pipeline
    def requires(self):
        return DownloadData(
            data_name=self.data_name, dest_folder=self.dest_folder
        )

    def output(self):
        return luigi.LocalTarget(
            os.path.join(self.dest_folder, self.data_name, "extracts.txt")
        )

    def run(self):
        # create another folder for archive
        ds_dir = os.path.join(self.dest_folder, self.data_name)
        os.makedirs(ds_dir, exist_ok=True)

        # Open tarfile
        with tarfile.open(self.input().path, "r") as tar:
            # Iterate tar
            for member in tar.getmembers():
                # Check for gz extension
                if member.isfile() and member.name.endswith(".gz"):
                    # get basename
                    gz_file_name = os.path.basename(member.name)

                    # Create dir for file (if absent)
                    folder_name = os.path.join(ds_dir, gz_file_name[:-7])
                    os.makedirs(folder_name, exist_ok=True)

                    # Extract and save
                    with tar.extractfile(member) as gz_file:
                        if gz_file is not None:
                            output_path = os.path.join(folder_name, gz_file_name[:-3])
                            with gzip.open(gz_file, "rb") as f_in:
                                with open(output_path, "wb") as f_out:
                                    f_out.write(f_in.read()) 

                    # Get 4 tables from file
                    self.separate_tables(output_path)

        with self.output().open("w") as f:
            f.write(f"Base archive is in the folder: {ds_dir}\n")
            f.write(f"Tables are extracted from files and written in separate .tsv files")

    # Convert tables into .tsv files
    def separate_tables(self, fname):
        dfs = {}
        with open(fname, "r") as f:
            write_key = None
            fio = io.StringIO()
            for l in f.readlines():
                if l.startswith("["):
                    if write_key:
                        fio.seek(0)
                        header = None if write_key == "Heading" else "infer"
                        dfs[write_key] = pd.read_csv(fio, sep="\t", header=header)
                    fio = io.StringIO()
                    write_key = l.strip("[]\n")
                    continue
                if write_key:
                    fio.write(l)
            fio.seek(0)
            dfs[write_key] = pd.read_csv(fio, sep="\t")

        # save into separate .tsv files
        for key, df in dfs.items():
            output_file = Path(fname).parent / f"{key}.tsv"
            df.to_csv(output_file, sep="\t", index=False)


class DataPreprocessing(luigi.Task):
    """
    Data preprocessing:
    - from prep_list delete cols_fordrop
    - save new files with "_preprocess" postfix
    """

    data_name = luigi.Parameter()
    dest_folder = luigi.Parameter()
    prep_list = luigi.ListParameter()
    cols_fordrop = luigi.ListParameter()

    # chain with ExtractFiles task,
    # because of Probes table
    def requires(self):
        return ExtractFiles(
            data_name=self.data_name, dest_folder=self.dest_folder
        )

    def output(self):
        ds_dir = Path(os.path.join(self.dest_folder, self.data_name))
        files = []
        for pattern in self.prep_list:
            f_preprocess = list(ds_dir.rglob(pattern))
            if f_preprocess:
                for fl in f_preprocess:
                    name, ext = os.path.splitext(fl)
                    files.append(f"{name}_preprocessed{ext}")

        return [luigi.LocalTarget(file_name) for file_name in files]

    def run(self):
        ds_dir = Path(os.path.join(self.dest_folder, self.data_name))

        # find all needed files inside ds_dir
        for pattern in self.prep_list:
            # get path list for founded files
            f_preprocess = list(ds_dir.rglob(pattern))
            if f_preprocess:
                for fl in f_preprocess:
                    # get daraframes from files
                    df = pd.read_csv(fl, sep="\t")
                    # drop useless columns
                    df_prep = df.drop(columns=list(self.cols_fordrop), errors="ignore")
                    # set new file names and save
                    name, ext = os.path.splitext(fl)
                    df_prep.to_csv(f"{name}_preprocessed{ext}", sep="\t", index=False)


class DelTempFiles(luigi.Task):
    """
    Delete artefacts:
    - downloaded file;
    - txt files from archives
    """

    data_name = luigi.Parameter(default="GSE68849")
    dest_folder = luigi.Parameter(default="data")
    prep_list = luigi.ListParameter(default=["Probes.tsv"])
    cols_fordrop = luigi.ListParameter(
        default=[
            "Definition",
            "Ontology_Component",
            "Ontology_Process",
            "Ontology_Function",
            "Synonyms",
            "Obsolete_Probe_Id",
            "Probe_Sequence",
        ]
    )

    def requires(self):
        return DataPreprocessing(
            data_name=self.data_name,
            dest_folder=self.dest_folder,
            prep_list=self.prep_list,
            cols_fordrop=self.cols_fordrop,
        )

    def output(self):
        # get log of success inside file
        return luigi.LocalTarget(
            Path(self.dest_folder) / self.data_name / "done.txt"
        )

    def run(self):
        ds_dir = Path(os.path.join(self.dest_folder, self.data_name))
        del_files = []

        # delete tar file and txt files
        path_ds = os.path.join(self.dest_folder, f"{self.data_name}.tar")
        if os.path.exists(path_ds):
            os.remove(path_ds)
            del_files.append(path_ds)

        for txt_file in ds_dir.rglob("*.txt"):
            os.remove(txt_file)
            del_files.append(txt_file)

        # save logs
        with self.output().open("w") as f:
            f.write("Pipeline completed.\n Temporary files deleted:\n")
            for fl in del_files:
                f.write(f" - {fl}\n")


class CreateDataSet(luigi.WrapperTask):
    """
    Wrapper.
    """

    data_name = luigi.Parameter(default="GSE68849")
    dest_folder = luigi.Parameter(default="data")
    prep_list = luigi.ListParameter(default=["Probes.tsv"])
    cols_fordrop = luigi.ListParameter(
        default=[
            "Definition",
            "Ontology_Component",
            "Ontology_Process",
            "Ontology_Function",
            "Synonyms",
            "Obsolete_Probe_Id",
            "Probe_Sequence",
        ]
    )

    def requires(self):
        return DelTempFiles(
            data_name=self.data_name,
            dest_folder=self.dest_folder,
            prep_list=self.prep_list,
            cols_fordrop=self.cols_fordrop,
        )


if __name__ == "__main__":
    luigi.run()
