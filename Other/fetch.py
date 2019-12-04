import requests
import re
import time

"""

  .oooooo.   ooooo        oooooooooooo       .o.       ooooo      ooo 
 d8P'  `Y8b  `888'        `888'     `8      .888.      `888b.     `8' 
888           888          888             .8"888.      8 `88b.    8  
888           888          888oooo8       .8' `888.     8   `88b.  8  
888           888          888    "      .88ooo8888.    8     `88b.8  
`88b    ooo   888       o  888       o  .8'     `888.   8       `888  
 `Y8bood8P'  o888ooooood8 o888ooooood8 o88o     o8888o o8o        `8  
                                                                      
                                                                      
                                                                      
ooooooooooooo oooooooooooo ooooooo  ooooo ooooooooooooo 
8'   888   `8 `888'     `8  `8888    d8'  8'   888   `8 
     888       888            Y888..8P         888      
     888       888oooo8        `8888'          888      
     888       888    "       .8PY888.         888      
     888       888       o   d8'  `888b        888      
    o888o     o888ooooood8 o888o  o88888o     o888o     
                                                      
                                                      """
def normalize_text(text):
    """Normalize Text
    """
    text = '\n'.join(
        text.splitlines())  # Let python take care of unicode break lines

    # Convert to upper
    text = text.upper()  # Convert to upper

    # Take care of breaklines & whitespaces combinations due to beautifulsoup parsing
    text = re.sub(r'[ ]+\n', '\n', text)
    text = re.sub(r'\n[ ]+', '\n', text)
    text = re.sub(r'\n+', '\n', text)

    # To find MDA section, reformat item headers
    text = text.replace('\n.\n', '.\n')  # Move Period to beginning

    text = text.replace('\nI\nTEM', '\nITEM')
    text = text.replace('\nITEM\n', '\nITEM ')
    text = text.replace('\nITEM  ', '\nITEM ')

    text = text.replace(':\n', '.\n')

    # Math symbols for clearer looks
    text = text.replace('$\n', '$')
    text = text.replace('\n%', '%')

    # Reformat
    text = text.replace('\n', '\n\n')  # Reformat by additional breakline

    return text


"""
  .oooooo.    oooooooooooo ooooooooooooo 
 d8P'  `Y8b   `888'     `8 8'   888   `8 
888            888              888      
888            888oooo8         888      
888     ooooo  888    "         888      
`88.    .88'   888       o      888      
 `Y8bood8P'   o888ooooood8     o888o     
                                         
                                         
                                         
ooo        ooooo oooooooooo.         .o.       
`88.       .888' `888'   `Y8b       .888.      
 888b     d'888   888      888     .8"888.     
 8 Y88. .P  888   888      888    .8' `888.    
 8  `888'   888   888      888   .88ooo8888.   
 8    Y     888   888     d88'  .8'     `888.  
o8o        o888o o888bood8P'   o88o     o8888o 
                                               
"""
def parse_mda(origText, start=0, reverse=False, remove=0):
    text = origText
    parStart = time.time()
    debug = False
    """Parse normalized text 
    """

    mda = ""
    end = 0
    """
        Parsing Rules
    """

    # Define start & end signal for parsing
    item7_begins = [">ITEM 7. MANAGEMENT", ">ITEM 7. MANAGEMENT",
        '>ITEM 7.','>ITEM&NBSP;7.', ">ITEM&#160;7.", ">ITEM 7 MANAGEMENT", ">Item 7 - Management",'ITEM 7.','ITEM&NBSP;7.', "ITEM&#160;7.", "ITEM 7 MANAGEMENT", "ITEM 7 - Management", ">ITEM 7", "> ITEM 7"]
    item7_begins=item7_begins[remove+1:]
    if reverse:
        item7_begins.reverse()
        
    item7_ends = ['>ITEM 8.','>ITEM&NBSP;8.', ">ITEM&#160;8.", ">ITEM 8 MANAGEMENT", ">Item 8 - Management",'ITEM 8.','ITEM&NBSP;8.', "ITEM&#160;8.", "ITEM 8 MANAGEMENT", "Item 8 - Management"]
    """
        Parsing code section
    """
    text = text[start:]

    
    # Get begin
    print("Time to start iteration: ", time.time()-parStart)
    for h in range(0,len(item7_begins)):
        item7 = item7_begins[h]
        #begin = find_2nd(text, item7)
        #print(begin)
        #if begin <3:
            #begin = text.find(item7)
        
        begin = text.rfind(item7)
        if (begin <= len(text)/100) or (begin >= len(text)-1000):
            continue

        if debug:
            print(item7, begin)
        if begin != -1:
            text = text[begin+1:]
            break
    if begin ==-1:
        #print(text)
        #afdafd
        return ""
    if begin != -1:  # Begin found
        for item7A in item7_ends:
            end = text.find(item7A)
            if debug:
                print(item7A, end)
            if end != -1:
                text = text[:end]
                break

        # Get MDA
        if end > -1:
            mda = text.strip()
            mda = text.replace("\n", " ")
        else:
            end = 0
    print("LEN MDA: ", len(mda))
    print("LEN TEXT: ", len(origText))
    if len(mda)>(len(origText)/12):
        print("Running MDA again")
        print(len(origText))
        #print("B",begin)
        #print(end)
        #print(origText.find(origText[begin:end]))
        #print(len(origText.replace(origText[begin:end], "")))
        if end>0:
            mda = parse_mda(origText.replace(origText[begin:begin+end],""))
        else:
            mda = parse_mda(origText.replace(origText[begin:], ""))
        print("LEN NEW MDA: ", len(mda))
        return " "
    if len(mda) < 5000 and len(mda)>0:
        print("RUNNING MDA AGAIN")
        if end>0:
            mda = parse_mda(origText.replace(origText[begin:begin+end],""))
        else:
            mda = parse_mda(origText.replace(origText[begin:], ""))
        print("LEN NEW MDA: ", len(mda))
        print(len(mda))
        
    return mda


def f(url):
    words = url[1]
    words = words[2:]
    url = url[0]
    companyData=[]
    text = requests.get(url).text
    text = normalize_text(text)
    mda = parse_mda(text)
    if len(mda)<500:
        return None
    for word in words:
        #print(word)
        companyData.append(mda.count(word))
       
    return companyData