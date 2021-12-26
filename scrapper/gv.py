import requests
from bs4 import BeautifulSoup


url = """
https://www.cnbc.com/2021/12/10/amc-shares-slump-as-ceo-adam-aron-cfo-sean-goodman-sell-stock.html

"""
s= requests.Session()
page = s.get(url)
# parse bs4
soup = BeautifulSoup(page.content, 'html.parser')

# print(soup)
# print(soup.select(".article-header__headline")[0].text)
print(soup.select("div.RenderKeyPoints-list")[0].text)
print(soup.select("picture>img")['src'])
print(soup.select(".ArticleBody-articleBody>.group"))


