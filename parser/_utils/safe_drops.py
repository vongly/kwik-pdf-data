# pouch/envelope is sometimes blank
# fill in as 'blank'
def fill_in_missing_pouch_envelope(word_list, missing_value='blank'):
    empty_pouch_env_index = []
    for i, word in enumerate(word_list):
        if '$' in word:
            if word_list[i-1] in ['AM', 'PM'] or word_list[i-1].startswith('end') or i == 0:
                empty_pouch_env_index.append(i)
            
    if empty_pouch_env_index:
        insertions = [ (i, missing_value) for i in empty_pouch_env_index ]

        for idx, val in sorted(insertions, reverse=True):
            word_list.insert(idx, val)
    
    return word_list