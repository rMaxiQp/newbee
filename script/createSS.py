import os
import time
from selenium import webdriver
#std = file:///Users/xyr1217/Desktop/test/videoScreenshot/2001/01-2001.html
dir_path="/Users/xyr1217/Desktop/test/2010/"
def save_map(m,filename,pngname):
    browser = webdriver.Firefox()
    delay = 4
    fn=filename
    tmpurl='file://{path}/{mapfile}'.format(path=os.getcwd(),mapfile=fn)
    m.save(fn)
    #use chrome driver, if you have not used it before, download it at https://sites.google.com/a/chromium.org/chromedriver/downloads
    browser.get(tmpurl)
    #Give the map tiles some time to load
    time.sleep(delay)
    #take screenshot
    browser.save_screenshot(dir_path+pngname)
    #browser.quit()
    browser.quit()

def testSelenium():
    print("browser will open")
    browser = webdriver.Chrome()
    print("browser has opened")
    delay = 6
    browser.get("http://ms.9you.com")
    time.sleep(delay)
    browser.save_screenshot("demo.png")
    print("browser will close")
    browser.quit()
    print("browser closed")

def create_screenshot(filename):
    browser = webdriver.Chrome()
    delay = 3
    fileurl = "file://" + dir_path + filename + ".html"
    # print(fileurl)
    browser.get(fileurl)
    time.sleep(delay)
    browser.save_screenshot("/Users/xyr1217/Desktop/test/2010img/" + filename + ".png")
    browser.quit()
    print("browser closed")

for file in os.listdir(dir_path):
    filename = file.split('.')
    create_screenshot(filename[0])
