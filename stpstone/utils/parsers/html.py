from requests import HTTPError, Response
from bs4 import BeautifulSoup
from lxml import html, etree
from typing import Union


class HtmlHandler:

    def bs_parser(self, resp_req: Response, parser:str="html.parser") -> Union[BeautifulSoup, str]:
        try:
            return BeautifulSoup(resp_req.content, parser)
        except HTTPError as e:
            return "HTTP Error: {}".format(e)

    def lxml_parser(self, resp_req:Response) -> html.HtmlElement:
        page = resp_req.content
        return html.fromstring(page)

    def lxml_xpath(self, html_content, str_xpath):
        return html_content.xpath(str_xpath)

    def html_tree(self, html_root:html.HtmlElement, file_path:str=None) -> None:
        html_string = etree.tostring(html_root, pretty_print=True, encoding="unicode")
        if file_path:
            with open(file_path, "w", encoding="utf-8") as file:
                file.write(html_string)
        else:
            print(html_string)

    def to_txt(self, html_):
        soup = BeautifulSoup(html_, features="lxml")
        return soup(html_)

    def parse_html_to_string(self, html_, parsing_lib="html.parser",
                             str_body_html="",
                             join_td_character="|", td_size_ajust_character=" "):
        # setting variables
        list_ = list()
        list_tr_html = list()
        dict_ = dict()
        dict_fill_blanks_td = dict()
        # creating a parseable object
        obj_soup = BeautifulSoup(html_, parsing_lib)
        html_parsed_raw = obj_soup.get_text()
        # creating a raw parsed html body
        list_body_html = html_parsed_raw.split("\n")
        # looping through tables and periods in the raw parsed html body
        for str_ in list_body_html:
            #   append to tr, provided the value is different from empty, what is an indicative of
            #       line scape
            if str_ != "":
                list_.append(str_)
            else:
                if len(list_) > 0:
                    list_tr_html.append(list_)
                list_ = list()
            #   add tr to the list, without reseting the intermediary list, provided it is the
            #       last instance of list body html
            if (str_ == list_body_html[-1]) and (len(list_) > 0):
                list_tr_html.append(list_)
        # looping through each tr to find the maximum td length
        for i in range(len(list_tr_html)):
            #   if tr length is greater than 1 its a sign of a row from a table, otherwise its is
            #   considered a period from a phrase
            if len(list_tr_html[i]) > 1:
                dict_[i] = {j: len(list_tr_html[i][j])
                            for j in range(len(list_tr_html[i]))}
        # build dictionary with blank spaces, aiming to reach columns of same size
        for _, dict_j in dict_.items():
            for j, _ in dict_j.items():
                dict_fill_blanks_td[j] = max([dict_[i][j]
                                              for i in list(dict_.keys()) if i in dict_ and j in
                                              dict_[i]])
        # joining td"s with a separator
        for i in range(len(list_tr_html)):
            #   filling blanks to construct columns of the same size
            str_body_html += join_td_character.join([list_tr_html[i][j]
                                                     + td_size_ajust_character *
                                                     (dict_fill_blanks_td[j] -
                                                      len(list_tr_html[i][j]))
                                                     for j in range(len(list_tr_html[i]))])
            #   adding line scapes
            try:
                if len(list_tr_html[i]) == len(list_tr_html[i + 1]):
                    str_body_html += "\n"
                else:
                    str_body_html += 2 * "\n"
            except IndexError:
                continue
        # returning html body parsed
        return str_body_html


class HtmlBuilder:

    def tag(self, name, *content, cls=None, **attrs):
        """
        REFERENCES: - FLUENT PYTHON BY LUCIANO RAMALHO (O’REILLY). COPYRIGHT 2015 LUCIANO RAMALHO, 978-1-491-94600-8.
        DOCSTRINGS: HTML TAG CONSTRUCTOR
        INPUTS: *ARGUMENTS, AND **ATTRIBUTES, BESIDE A CLS WORKAROUND SINCE CLASS IS A SPECIAL
            WORD FOR PYTHON
        OUTPUTS: STRING
        """
        # defining tag & method
        if cls is not None:
            attrs["class"] = cls
        if attrs:
            attr_str = ''.join(' {}="{}"'.format(attr, value) for attr, value
                               in sorted(attrs.items()))
        else:
            attr_str = ''
        # defining element
        if content:
            return '\n'.join('<{}{}>{}</{}>'.format(name, attr_str, c,
                                                    name) for c in content)
        else:
            return '<{}{} />'.format(name, attr_str)
