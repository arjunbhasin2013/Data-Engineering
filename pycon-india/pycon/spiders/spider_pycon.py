# -*- coding: utf-8 -*-

import scrapy
from scrapy.http import Request

class PyConSpider(scrapy.Spider):
    name = "pycon"
    start_urls  = ['https://in.pycon.org/cfp/2016/proposals/',
    'https://in.pycon.org/cfp/2017/proposals/',
    'https://in.pycon.org/cfp/2018/proposals/',
    'https://in.pycon.org/cfp/2019/proposals/',
    ]
    
    def parse(self, response):
        url_links_resp = response.css('h3 a::attr(href)').extract()
        comments_response = response.css('span[class=align-middle]').extract()
        
        comments_clean = [idx.split('<span class="align-middle">\n')[1].split('</span>')[0].strip() for idx in comments_response]
        
        url_ls = [str('https://in.pycon.org')+idx for idx in url_links_resp]
        return (Request(str(url), callback=self.parse_page) for url in url_ls)
    
    def remove_html_tags(self,text):
        """Remove html tags from a string"""
        import re
        clean = re.compile('<.*?>')
        return str(re.sub(clean, '', text)).replace('\n', '').replace(':','').strip()
        
    def parse_page(self, response):
        processing_url = response.url
        title = response.xpath('//div[@class="col-sm-12 proposal-header"]/h1[@class="proposal-title"]').extract()
        votes_count = response.xpath('//h1[@class="clear-margin text-muted vote-count"]').extract()
        target_audience = response.css('td').extract()
        writeup_section = response.css('div[class=proposal-writeup--section]').extract()
        datetime_author = response.xpath('//div[@class="col-sm-12 proposal-header"]//p').extract()[0].split('>')
        
        try:
            result_dict['Title'] = title[0].split('<h1 class="proposal-title">')[1].split('</h1>')[0].replace("\n", '').strip()
            result_dict['Author'] = datetime_author[5].split('</b')[0].replace('\n','').strip()
            result_dict['Publish_Date'] = datetime_author[10].split('</time')[0].strip()
            result_dict['URL'] = processing_url
            result_dict['Target_Audience'] = target_audience[5].split('<td>')[1].split('</td>')[0].strip()
            result_dict['Type'] = target_audience[3].split('<td>')[1].split('</td>')[0].strip()
            result_dict['Section'] = target_audience[1].split('<td>')[1].split('</td>')[0].strip()
            result_dict['Last_Updated'] = target_audience[7].split('title=')[1].split('</time>')[0].split('>')[0].replace('"','')
            if len(votes_count)>0:
                result_dict['Votes_Count'] = str(votes_count).split('<h1 class="clear-margin text-muted vote-count">')[1].split('</h1>')[0].replace("\\n",'').strip()
            else:
                result_dict['Votes_Count'] = 'NA'
            result_dict['Description'] = self.remove_html_tags(str(writeup_section[0].split('Description:')[1].split('</div>')[0]))
            result_dict['Prerequisites'] = self.remove_html_tags(str(writeup_section[1].split('Prerequisites:')[1].split('</div>')[0]))
            result_dict['Content_URLs'] = self.remove_html_tags(str(writeup_section[2].split('Content URLs:')[1].split('</div>')[0]))
            result_dict['Speaker_Info'] = self.remove_html_tags(str(writeup_section[3].split('Speaker Info')[1].split('</div>')[0]))
            result_dict['Speaker_Links'] = self.remove_html_tags(str(writeup_section[4].split('Speaker Links')[1].split('</div>')[0]))
            yield result_dict
            
        except:
            pass
        

        
        
    