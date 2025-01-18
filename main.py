import string
from collections import defaultdict, Counter
import os
import shutil
import aiohttp
import nest_asyncio
import asyncio
from matplotlib import pyplot as plt

# Enable nested event loops
nest_asyncio.apply()

async def get_text(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                return await response.text()
            return None

# Function to remove punctuation
def remove_punctuation(text):
    return text.translate(str.maketrans("", "", string.punctuation))

async def map_function(word):
    return word.lower(), 1  # Convert to lowercase for better analysis

def shuffle_function(mapped_values):
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()

async def reduce_function(key_values):
    key, values = key_values
    return key, sum(values)

# Perform MapReduce
async def map_reduce(url):
    # Get and clean text
    text = await get_text(url)
    if text is None:
        raise ValueError("Failed to fetch text from URL")
    
    text = remove_punctuation(text)
    words = text.split()

    # Parallel Mapping
    mapped_result = await asyncio.gather(*[map_function(word) for word in words])

    # Shuffle
    shuffled_values = shuffle_function(mapped_result)

    # Reduce
    reduced_result = await asyncio.gather(
        *[reduce_function(values) for values in shuffled_values]
    )
    return dict(reduced_result)

def visualize_top_words(result, top_n=10):
    # Get top-N most frequent words, excluding very common words
    stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by'}
    filtered_words = {word: count for word, count in result.items() if word.lower() not in stop_words}
    top_words = Counter(filtered_words).most_common(top_n)

    # Розділення даних на слова та їх частоти
    words, counts = zip(*top_words)

    # Створення графіка
    plt.figure(figsize=(10, 6))
    plt.barh(words, counts, color='skyblue')
    plt.xlabel('Frequency')
    plt.ylabel('Words')
    plt.title('Top {} Most Frequent Words'.format(top_n))
    plt.gca().invert_yaxis()  # Перевернути графік, щоб найбільші значення були зверху
    plt.show()

def generate_readme(result, top_n=10):
    # Define the path for the plot image
    plot_image_path = "./fig/top_words_plot.png"

    # Save the plot
    visualize_top_words(result, top_n)
    plt.savefig(plot_image_path, bbox_inches='tight', dpi=300)
    plt.close()

    # Generate README content
    readme_content = """
# Word Frequency Analysis

This project performs a word frequency analysis on the text of "Sherlock Holmes" by Arthur Conan Doyle, using asyncio and MapReduce principles.

## Results

The table below shows the top {} most frequent words (excluding common words) found in the text:

| Word | Frequency |
|------|-----------|
""".format(top_n)

    # Add the top words and frequencies to the table
    stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by'}
    filtered_words = {word: count for word, count in result.items() if word.lower() not in stop_words}
    top_words = Counter(filtered_words).most_common(top_n)
    
    for word, count in top_words:
        readme_content += f"| {word} | {count} |\n"

    readme_content += f"""
## Visualization

![Top Words Plot]({plot_image_path})

## Notes
- Common words (stop words) have been excluded from the analysis
- The text has been converted to lowercase for consistent counting
- Punctuation has been removed
"""

    # Write the README
    with open("README.md", "w", encoding='utf-8') as f:
        f.write(readme_content)

def main():
    url = "https://www.gutenberg.org/cache/epub/1661/pg1661.txt"
    
    # Create and get event loop
    loop = asyncio.get_event_loop()
    try:
        # Run the async function
        result = loop.run_until_complete(map_reduce(url))
        
        # Generate visualization and README
        visualize_top_words(result)
        generate_readme(result)
        print("Analysis completed successfully!")
        
        return result
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

if __name__ == "__main__":
    main()