import pprint

def unique_lists(a, b):
    seen = set(a)
    for x in b:
        if x not in seen:
            a.append(x)
            seen.add(x)
    return a

def merge_meta(meta1, meta2):

    for k2, v2 in meta2.iteritems():
        if k2 in meta1:
            v1 = meta1[k2]
            if type(v1) != type(v2):
                meta1[k2] = v2
                continue

            if isinstance(v1, dict):
                merge_meta(v1, v2)
            elif isinstance(v1, list):
                meta1[k2] = unique_lists(v1, v2)
            else:
                meta1[k2] = v2
        else:
            meta1[k2] = v2

def test(a, b):
    pprint.pprint(a)
    pprint.pprint(b)
    merge_meta(a, b)
    pprint.pprint(a)
    print '-'*20

def main():
    test({'a': 1, 'b': 2}, {'c':3})

    test({'a': 1, 'b': [1,2,3]}, {'b': [1,5]})

    test({'a': 1, 'b': [1,2,3]}, {'b': 3})

    test({'a': 1, 'b': [1,2]}, {'b': [1,3], 'c': [4,5]})

    test({'a': 1, 'b': {'a': 1}}, {'b': {'a':2}})

    test({'a': 1, 'b': {'c': 1, 'd': [3,4], 'e': {'f': [3]}}}, {'b': {'c':2, 'd': [2,5], 'e': {'f': [1]}}})

if __name__ == "__main__":
    main()

