# usage: python3 get_seqs.py nodes.txt > seqs.fasta

import csv
import pandas as pd
import requests
import sys
import stringdb_virus
import sys


def get_seq(cursor, ext_id):
    [taxid, name] = ext_id.split('.', 1)
    taxid = int(taxid)

    cursor.execute("""
        select distinct ps.sequence
          from items.proteins p
          inner join items.proteins_sequences ps
            on p.protein_id = ps.protein_id
        where
          p.protein_external_id = %(ext_id)s
        ;
        """,
        {'ext_id': ext_id})

    res = cursor.fetchall()

    seqs = set()

    if res:
        seqs = {seq for seq, in res}

    else:
        cursor.execute("""
            select p.protein_id, pn.protein_name
              from items.proteins p
              right join items.proteins_names pn
                on p.protein_id = pn.protein_id
            where
              p.protein_external_id = %(ext_id)s
              and
              pn.source = 'UniProtKB-EI'
            ;
            """,
            {'ext_id': ext_id})

        alts = cursor.fetchall()

        if len(alts) == 0:
            print(f'no alternatives found for {ext_id}', file=sys.stderr)
            return None

        else:
            for string_id, prot_acc in alts:
                url = f'https://www.uniprot.org/uniprot/?query=accession:{prot_acc}&columns=sequence&format=tab'

                resp = requests.get(url)
                resp.raise_for_status()

                resp_lines = resp.text.splitlines()[1:]

                for resp_line in resp_lines:
                    resp_line = resp_line.strip()

                    if not resp_line:
                        continue

                    seqs.add(resp_line)

            if len(seqs) == 0:
                print(f'!! no sequences found for {ext_id} (alternatives: {alts})', file=sys.stderr)
                return None

    # if len(seqs) > 1:
    #     print(f'fetched {len(seqs)} > 1 sequences for {ext_id}', file=sys.stderr)
    #     for i, seq in enumerate(seqs):
    #         print(f' {i}:', seq, file=sys.stderr)

    max_seq = max(seqs, key=len)
    return max_seq


def get_seqs(cursor, ext_ids):
    results = []

    nones = 0

    for ext_id in ext_ids:
        seq = get_seq(cursor, ext_id)

        if seq is None:
            nones += 1
        else:
            results.append((ext_id, seq))

    return pd.DataFrame(results, columns=['external_id', 'sequence'])


def write_seqs_fasta(seqs, f):
    for row in seqs.itertuples():
        print(f'> {row.external_id}', file=f)
        print(row.sequence, file=f)


if __name__ == '__main__':
    with open(sys.argv[1]) as f:
        ext_ids = {line.strip() for line in f}
        ext_ids = {ext_id for ext_id in ext_ids if ext_id}

    string_conn = stringdb_virus.connect_to_docker()
    cursor = string_conn.cursor()

    write_seqs_fasta(get_seqs(cursor, ext_ids), sys.stdout)
