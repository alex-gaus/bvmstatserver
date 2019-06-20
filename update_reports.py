import dataset
from reports import get_reports

def update():
    db = dataset.connect("sqlite:///reports.db")
    reportsdb = db["reports"]
    # reportsdb.delete()
    reports = get_reports()
    for report in reports:
        reportsdb.upser(report,["report_link"])
    return True