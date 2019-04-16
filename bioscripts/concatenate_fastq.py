import logging
import os
import subprocess as sp
import sys
from datetime import datetime as d


def concat_fastqs(manifest, out_prefix):
    first_line = manifest[0].strip('\n').split(",")

    com1 = ["cat", first_line[0], ">", out_prefix + "_1.fq.gz", ";",
            "gunzip -c ", first_line[0], "| wc -l >", out_prefix + "_1_lines.txt"]
    com2 = ["cat", first_line[1], ">", out_prefix + "_2.fq.gz", ";",
            "gunzip -c ", first_line[1], "| wc -l >", out_prefix + "_2_lines.txt"]
    fname_r1 = os.path.basename(first_line[0])
    fname_r2 = os.path.basename(first_line[1])

    line_cnt_r1 = sp.check_output(' '.join(com1), shell=True)
    line_cnt_r2 = sp.check_output(' '.join(com2), shell=True)

    logging.info('\t'.join([fname_r1, line_cnt_r1, fname_r2, line_cnt_r2]))
    logging.info('\t' + ' '.join(com1))
    logging.info('\t' + ' '.join(com2))

    for line in manifest[1:]:
        tmpline = line.strip('\n').split(",")
        fname_r1 = os.path.basename(tmpline[0])
        fname_r2 = os.path.basename(tmpline[1])
        com1 = ["cat", tmpline[0], ">>", out_prefix + "_1.fq.gz", ";",
                "gunzip -c ", tmpline[0], "| wc -l >>", out_prefix + "_1_lines.txt"]
        com2 = ["cat", tmpline[1], ">>", out_prefix + "_2.fq.gz", ";",
                "gunzip -c ", tmpline[1], "| wc -l >>", out_prefix + "_2_lines.txt"]
        line_cnt_r1 = sp.check_output(' '.join(com1), shell=True)
        line_cnt_r2 = sp.check_output(' '.join(com2), shell=True)
        logging.info('\t'.join([fname_r1, line_cnt_r1, fname_r2, line_cnt_r2]))
        logging.info('\t' + ' '.join(com1))
        logging.info('\t' + ' '.join(com2))

    com1 = ["gunzip -c ", out_prefix + "_1.fq.gz", "| wc -l >>", out_prefix + "_1_lines.txt"]
    com2 = ["gunzip -c ", out_prefix + "_2.fq.gz", "| wc -l >>", out_prefix + "_2_lines.txt"]
    line_cnt_r1 = sp.check_output(' '.join(com1), shell=True)
    line_cnt_r2 = sp.check_output(' '.join(com2), shell=True)
    logging.info('\t'.join([fname_r1, line_cnt_r1, fname_r2, line_cnt_r2]))
    logging.info('\t' + ' '.join(com1))
    logging.info('\t' + ' '.join(com2))
    return


def manifest_from_file(infile):
    """
    :param infile: A CSV file that contains 3 columns sample, path for read1 file, path for read2 file
    :return:
    """
    manifest = open(infile, 'r').readlines()
    return manifest


def rename_concat_fastqs(manifest, out_prefix, batch=False):
    os.mkdir(os.path.join(out_prefix, "verify"))
    samp_fastq_r1 = dict()
    samp_fastq_r2 = dict()
    commands = dict()
    for line in manifest:
        tmp_line = line.strip('\n').split(",")
        if tmp_line[0] not in samp_fastq_r1.keys():
            samp_fastq_r1[tmp_line[0]] = [tmp_line[1]]
            samp_fastq_r2[tmp_line[0]] = [tmp_line[2]]
        else:
            samp_fastq_r1[tmp_line[0]].append(tmp_line[1])
            samp_fastq_r2[tmp_line[0]].append(tmp_line[2])

    for samp in samp_fastq_r1.keys():
        if len(samp_fastq_r1[samp]) < 2:
            fq1 = samp_fastq_r1[samp][0]
            fq2 = samp_fastq_r2[samp][0]
            com = ["cat ", fq1, ">", out_prefix + "/" + samp + "_1.fq.gz", ";",
                   "gunzip -c ", fq1, "| wc -l >", out_prefix + "/verify/" + samp + "_1_lines.txt", ";",
                   "gunzip -c ", fq1, "| wc -l >", out_prefix + "/verify/" + samp + "_1_lines.txt", ";",
                   "cat ", fq2, ">", out_prefix + "/" + samp + "_2.fq.gz", ";",
                   "gunzip -c ", fq2, "| wc -l >", out_prefix + "/verify/" + samp + "_2_lines.txt", ";"]

            com += ["gunzip -c ", out_prefix + "/" + samp + "_1.fq.gz", "| wc -l >>",
                    out_prefix + "/verify/" + samp + "_1_lines.txt", ";",
                    "gunzip -c ", out_prefix + "/" + samp + "_2.fq.gz", "| wc -l >>",
                    out_prefix + "/verify/" + samp + "_2_lines.txt", ";"]

            commands[samp] = ' '.join(com)
        else:
            fq1 = samp_fastq_r1[samp][0]
            fq2 = samp_fastq_r2[samp][0]
            com = ["cat ", fq1, ">", out_prefix + "/" + samp + "_1.fq.gz", ";",
                   "gunzip -c ", fq1, "| wc -l >", out_prefix + "/verify/" + samp + "_1_lines.txt", ";",
                   "cat ", fq2, ">", out_prefix + "/" + samp + "_2.fq.gz", ";",
                   "gunzip -c ", fq2, "| wc -l >", out_prefix + "/verify/" + samp + "_2_lines.txt", ";"]

            for s in range(1, len(samp_fastq_r1[samp])):
                fq1 = samp_fastq_r1[samp][s]
                fq2 = samp_fastq_r2[samp][s]
                com += ["cat ", fq1, ">>", out_prefix + "/" + samp + "_1.fq.gz", ";",
                        "gunzip -c ", fq1, "| wc -l >>", out_prefix + "/verify/" + samp + "_1_lines.txt", ";"
                                                                                                          "cat ", fq2,
                        ">>", out_prefix + "/" + samp + "_2.fq.gz", ";",
                        "gunzip -c ", fq2, "| wc -l >>", out_prefix + "/verify/" + samp + "_2_lines.txt", ";"]

            com += ["gunzip -c ", out_prefix + "/" + samp + "_1.fq.gz", "| wc -l >>",
                    out_prefix + "/verify/" + samp + "_1_lines.txt", ";",
                    "gunzip -c ", out_prefix + "/" + samp + "_2.fq.gz", "| wc -l >>",
                    out_prefix + "/verify/" + samp + "_2_lines.txt", ";"]

            commands[samp] = ' '.join(com)

    for samp, com in commands.iteritems():
        logging.info(samp + ":\n\t" + com + "\n")
        if not batch:
            sp.check_output(com, shell=True)
        else:
            slurm_dir = os.path.join(out_prefix, "slurm_log")
            if not os.path.exists(slurm_dir):
                os.mkdir(slurm_dir)
            com1 = "sbatch -t 2:00:00 -J " + samp + " -o" + slurm_dir + "/" + samp + "_%j.out -e" + slurm_dir + "/" + samp + "_%j.err --wrap=' " + com + " ' "
            sp.check_output(com1, shell=True)
    return


if __name__ == "__main__":
    logfile = "concatenate_fastq_" + os.path.basename(sys.argv[2]) + d.now().strftime("_%Y_%m_%d_%H_%M") + ".log"
    print logfile
    logging.basicConfig(filename=logfile, filemode='w', format='%(levelname)s - %(message)s',
                        level=logging.INFO)
    input_manifest = manifest_from_file(sys.argv[1])
    if sys.argv[3] == "rename":
        try:
            if sys.argv[4] == "batch":
                rename_concat_fastqs(input_manifest, sys.argv[2], batch=True)
        except:
            rename_concat_fastqs(input_manifest, sys.argv[2])
    else:
        concat_fastqs(input_manifest, sys.argv[2])
