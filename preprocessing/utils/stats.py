from datasets import load_dataset

def get_number_files(dataset, language):
    """Get number of files in dataset corresponding to a language"""
    ds = dataset.filter(lambda x: x["lang"] == language)
    nb_samples = len(ds)
    size = sum(ds['size']) 
    return {"nb_samples": nb_samples, "volume": size}

def get_size_text(example):
    return {"size": len(example["content"])}

# load dataset before filtering
dataset = load_dataset("bigcode/the-stack-pjjs-decontaminate", split="train", use_auth_token=True, num_proc=56)
dataset = dataset.map(get_size_text)

# load filtered data
dataset_filtered = load_dataset("bigcode/the-stack-stars-filter", split="train", use_auth_token=True, num_proc=56)

# get number of files per language

languages = ["Python", "Java", "Javascript"]
for language in languages:
    before = get_number_files(dataset, language)
    after = get_number_files(dataset_filtered, language)
    print(f"Number of files and volume in {language} before filtering:\n {before['nb_samples']} files, {before['volume'] / 1e9:.2f} GB")
    print(f"Number of files and volume in {language} after filtering:\n {after['nb_samples']} files, {after['volume'] / 1e9:.2f} GB")
    print(f"Number of files filtered in {language}: {(before['nb_samples'] - after['nb_samples']) * 100/before['nb_samples']:.2f}%")
    print(f"Volume filtered in {language}: {(before['volume'] - after['volume']) * 100/before['volume']:.2f}%")