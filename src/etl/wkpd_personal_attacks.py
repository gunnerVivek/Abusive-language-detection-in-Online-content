from pandas import read_csv
from definitions import DATA_DIR, TRANSFORMED_DATA_DIR
import os.path
from etl.definations_configurations import ABUSE, NO_ABUSE

def get_wikipedia_personal_attacks():
    
    folder_path = os.path.join(DATA_DIR, "Wikipedia Personal Attacks - conversational 3")

    attack_annotated_comments = os.path.join(folder_path, "attack_annotated_comments.tsv")

    # rev_id, comment, year, logged_in, ns, sample, split
    attack_annotated_comments = read_csv(attack_annotated_comments, sep='\t')
    attack_annotated_comments = attack_annotated_comments[['rev_id', 'comment']]


    attack_annotations = os.path.join(folder_path, "attack_annotations.tsv")
    # rev_d, worker_id, quoting_attack, recipient_attack, third_party_attack, other_attack, attack
    attack_annotations = read_csv(attack_annotations, sep='\t')
    attack_annotations = attack_annotations[['rev_id', 'attack']]

    # majority voting
    attack_annotations_avg = attack_annotations.groupby(['rev_id'])['attack'].mean().round(2)

    # remove dplicate rev_id
    attack_annotations = attack_annotations.drop_duplicates(subset=['rev_id']) 

    # mean value >= 0.5 label = 1 (attack) else 0 (no-attack)
    attack_annotations['label'] = attack_annotations.rev_id.apply(lambda x: ABUSE if attack_annotations_avg[x]>=0.5 else NO_ABUSE )
    attack_annotations = attack_annotations[['rev_id', 'label']]

    wkpd_personal_attack = pd.merge(attack_annotated_comments, attack_annotations, how='inner', on='rev_id')
    wkpd_personal_attack = wkpd_personal_attack[['comment', 'label']]

    return wkpd_personal_attack


if __name__ == "__main__":

    write_file = os.path.join(TRANSFORMED_DATA_DIR, 'wikipedia_personal_attacks.csv')

    data = get_wikipedia_personal_attacks()
    data.to_csv(write_file, index=False)
