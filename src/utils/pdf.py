import re
from bs4 import BeautifulSoup
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LAParams, LTTextContainer, LTChar, LTPage, LTTextBox, LTTextLine, LTAnno

def is_pdf(anchor):
        """Takes an anchor tag and returns true if the href
        ends with links to a .pdf file. Else returns false.

        Args:
            anchor (bs4.element.Tag): anchor tag
        """
        
        href = anchor.get("href")
        match = re.match(r".*\.pdf$", href)
        if match:
            return True
        else:
            return False

class CustomRoadMapConverter(PDFPageAggregator):
    def __init__(self, rsrcmgr, pageno=1, laparams=None):
        PDFPageAggregator.__init__(self, rsrcmgr, pageno=pageno, laparams=laparams)
        self.rows = []
        self.lines = []
        self.page_number = 0

    def custom_sort(self, ltpage):
        page_items = []
        to_sort_items = []
        for item in ltpage:
            if isinstance(item, LTTextBox) or isinstance(item, LTPage):
                # check if above certain threshold then parse as header
                # column parse
                if item.y1 > 740:
                    page_items.append(item)
                else:
                    to_sort_items.append(item)
        to_sort_items.sort(key=lambda x: round(x.x0))
        page_items.extend(to_sort_items)

                
        return page_items
    
    def receive_layout(self, ltpage):
        # page_items = self.custom_sort(ltpage)
        def render(item, page_number):
            if isinstance(item, LTPage) or isinstance(item, LTTextBox):
                for child in item:
                    render(child, page_number)
            elif isinstance(item, LTTextLine):
                line_elements = []
                curr_element = {}
                child_str = ''
                for child in item:
                    if isinstance(child, LTChar):
                        if (curr_element == {} or "size" not in curr_element.keys()):
                            curr_element['size'] = round(child.size)
                            curr_element['text'] = child.get_text()
                            curr_element['page'] = page_number
                        elif curr_element['size'] != round(child.size):
                            if curr_element['text']:
                                curr_element['text'] = ' '.join(curr_element['text'].split()).strip()
                                line_elements.append(curr_element)
                            curr_element = {"size": round(child.size), "text": child.get_text(), "page": page_number}
                        else:
                            curr_element['text'] += child.get_text()
                    elif isinstance(child, LTAnno):
                        if (curr_element == {}):
                            curr_element['text'] = child.get_text()
                            curr_element['page'] = page_number
                        else:
                            curr_element['text'] += child.get_text()
                line_elements.append(curr_element)
                self.lines.extend(line_elements)
                for child in item:
                    render(child, page_number)
            return
        render(ltpage, self.page_number)
        self.page_number += 1
        self.rows = sorted(self.rows, key = lambda x: (x[0], -x[2]))
        self.result = ltpage