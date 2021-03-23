import pandas as pd
import psycopg2
from pytrends.request import TrendReq

s1 = '1. Lấy dữ liệu trending'
s2 = '2. Xuất báo cáo top 10 trending'
s3 = '3. Xuất báo cáo search keyword in 2020'
s4 = '4. Vẽ biểu đồ line chart top 5 trending các từ khóa tìm kiếm nhiều nhất 2020'
s5 = '5. Vẽ biểu đồ bar chart top 5 trending các từ khóa tìm kiếm nhiều nhất 2019'
s6 = '6. Thống kê tìm kiếm top trending 5 từ khóa trong 2 năm 2020, 2019'
s99 = '99. Thoát'

s_help = "Vui lòng nhập số từ 1 đến 6, hoặc 99 để tiếp tục..."


def print_screen():
    print(s1, s2, s3, s4, s5, s6, s99, s_help, sep="\n")


def connect():
    """ Connect to the PostgreSQL database server """
    conn, cur = None, None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(
            host="localhost", port="5432",
            database="pos_test",
            user="postgres",
            password="admin")
        # create a cursor
        cur = conn.cursor()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while excuting SQL" + error)

    return conn, cur


def input_data():
    print(s1)
    f_name = input("Hãy nhập tên file, không gồm phần mở rộng:")
    if not f_name or len(f_name) == 0:
        f_name = 'keytrends'

    s_time_frame = input("Nhập khung thời gian timeframe:")
    if not s_time_frame or len(s_time_frame) == 0:
        s_time_frame = '2020-01-01 2020-12-31'

    pytrend = TrendReq(hl='VN', tz=360)
    try:
        kw_list_file = pd.read_excel(f_name + '.xls').dropna()
        column_names = list(kw_list_file.columns)
        conn, cur = connect()
        # cur.execute("DROP TABLE IF EXISTS vn_trending;")
        cur.execute('''CREATE TABLE  IF NOT EXISTS vn_trending(
                           date DATE,
                           keyword CHAR(100),
                           value INTEGER,
                           trend_type CHAR(100));'''
                    )
        conn.commit()
        print("Table created successfully")

        table = 'vn_trending'
        columns = "keyword, date, value, trend_type"
        for column in column_names:

            dataset = []
            df = kw_list_file[column].values.tolist()
            for index in range(len(df)):
                pytrend.build_payload(
                    kw_list=[df[index]],
                    cat=0, geo='VN',
                    timeframe=s_time_frame,
                    gprop='')
                data = pytrend.interest_over_time()
                if not data.empty:
                    data = data.drop(labels=['isPartial'], axis='columns')
                    dataset.append(data)

                    len_dt = len(data)
                    keyw = [data.columns[0]]
                    kws = len_dt * [str(keyw).replace("'", "''")]
                    dts1 = [dt.to_pydatetime().strftime('%Y-%m-%d %H:%M:%S') for dt in data.index]
                    vl = [d[0] for d in data.values]
                    insert_stmt = ''
                    for i in range(len_dt):
                        if vl[i] > 0 and df[index] != 'NaN':
                            values = "VALUES ('{}','{}','{}','{}')".format(kws[i], dts1[i], vl[i], column)
                            insert_stmt += "INSERT INTO {} ({}) ({});".format(table, columns, values)
                    cur.execute(insert_stmt)
                    conn.commit()
        conn.close()
    except Exception as ex:
        print("Có lỗi xảy ra trong quá trình đọc file, hoặc file không tồn tại, hoặc file không đúng định dạng!")
        print(ex)


def top_ten_trending():
    print(s2)
    sql = """
            SELECT row_number() over (ORDER BY A.sum_val DESC, A.keyword, B.monthly) as stt , 
                    A.keyword, A.sum_val, B.monthly, B.max_val
            FROM
                (	SELECT keyword, sum(VALUE) sum_val
                    FROM vn_trending
                    GROUP BY keyword
                    ORDER BY sum(VALUE::INT) DESC
                    LIMIT 10
                ) A
            JOIN 
                (
                    SELECT DISTINCT A1.keyword, A2.monthly, A1.max_val
                    FROM
                    (	SELECT keyword, max(sum_val) max_val
                        FROM
                        (
                            SELECT DISTINCT keyword, sum(VALUE) sum_val, to_char(date::date,'mm/yyyy') monthly
                            FROM vn_trending
                            GROUP BY keyword,to_char(date::date,'mm/yyyy') 
                            ORDER BY keyword, sum(VALUE::INT) DESC
                        ) A3 
                        GROUP BY keyword
                    ) A1
                    JOIN
                    (		SELECT DISTINCT keyword, sum(VALUE) sum_val, to_char(date::date,'mm/yyyy') monthly
                            FROM vn_trending
                            GROUP BY keyword,to_char(date::date,'mm/yyyy') 
                            ORDER BY keyword, sum(VALUE::INT) DESC
                        ) A2 ON A1.keyword = A2.keyword and A1.max_val = A2.sum_val
                ) B ON A.keyword = B.keyword
            ORDER BY A.sum_val DESC, A.keyword, B.monthly;"""
    conn, cur = connect()
    cur.execute(sql)
    rd = cur.fetchall()
    conn.close()
    cur.close()
    df = pd.DataFrame(rd,
                      columns=['STT', 'Từ khóa', 'Số lần tìm kiếm', 'Tháng tìm kiếm nhiều nhất', 'Số lần trong tháng'])
    writer = pd.ExcelWriter('vn_trending_top_ten.xlsx')
    df.to_excel(writer)
    print("Xuất thành công báo cáo Top ten trending")
    writer.save()


def search_key_word():
    print(s3)

    sql = """SELECT trend_type,keyword, to_char(date::date,'mm/yyyy') monthly, sum(VALUE) sum_val
                FROM vn_trending
                WHERE EXTRACT(YEAR FROM date::DATE) = 2020
                GROUP BY trend_type,keyword,to_char(date::date,'mm/yyyy') 
                ORDER BY trend_type, keyword;
            """
    conn, cur = connect()
    cur.execute(sql)
    rd = cur.fetchall()
    conn.close()
    cur.close()
    df = pd.DataFrame(rd, columns=['trend_type', 'keyword', 'monthly', 'sum_val'])
    df2 = df.pivot_table(index="keyword", columns="monthly", values='sum_val')
    writer = pd.ExcelWriter('vn_trending_search_keyword_2020.xlsx')
    df2.to_excel(writer)
    writer.save()


def top_five_trending(year='2019, 2020'):
    conn, cur = connect()
    if year == '2020':
        print(s4)
        sql = build_sql(year, 5)
        cur.execute(sql)
        rd = cur.fetchall()
        df = pd.DataFrame(rd, columns=['stt', 'keyword', 'sum_val', 'monthly', 'max_val'])
        if len(df):
            image = df.plot(title='TỪ KHÓA TÌM KIẾM NHIỀU NHẤT TẠI VIỆT NAM 2020')
            fig = image.get_figure()
            fig.savefig('top_search_key_2020.png')
    elif year == '2019':
        print(s5)
        sql = build_sql(year, 5)
        cur.execute(sql)
        rd = cur.fetchall()
        df = pd.DataFrame(rd, columns=['stt', 'keyword', 'sum_val', 'monthly', 'max_val'])
        if len(df):
            image = df.plot.bar(title='TỪ KHÓA TÌM KIẾM NHIỀU NHẤT TẠI VIỆT NAM 2019')
            fig = image.get_figure()
            fig.savefig('top_search_key_2019.png')
    else:
        print(s6)
        sql1 = build_sql('2019', 5)
        cur.execute(sql1)
        rd1 = cur.fetchall()
        df1 = pd.DataFrame(rd1, columns=['stt', 'keyword', 'sum_val', 'monthly', 'max_val'])
        sql2 = build_sql('2020', 5)
        cur.execute(sql2)
        rd2 = cur.fetchall()
        df2 = pd.DataFrame(rd2, columns=['stt', 'keyword', 'sum_val', 'monthly', 'max_val'])
        df = pd.concat([df1, df2], axis=1)

        writer = pd.ExcelWriter('vn_trending_2_year.xlsx')
        df.to_excel(writer)
        writer.save()
    conn.close()
    cur.close()


def build_sql(year=2020, limit=10):
    sql = """SELECT row_number() over (ORDER BY A.sum_val DESC, A.keyword, B.monthly) as stt , 
                    A.keyword, A.sum_val, B.monthly, B.max_val
            FROM
                (	SELECT keyword, sum(VALUE::INT) sum_val
                    FROM vn_trending
                    WHERE EXTRACT(YEAR FROM DATE) = %s
                    GROUP BY keyword
                    ORDER BY sum(VALUE::INT) DESC
                    LIMIT %s
                ) A
            JOIN 
                (
                    SELECT DISTINCT A1.keyword, A2.monthly, A1.max_val
                    FROM
                    (	SELECT keyword, max(sum_val) max_val
                        FROM
                        (
                            SELECT DISTINCT keyword, sum(VALUE::INT) sum_val, to_char(date::date,'mm/yyyy') monthly
                            FROM vn_trending
                            WHERE EXTRACT(YEAR FROM DATE) = %s
                            GROUP BY keyword,to_char(date::date,'mm/yyyy') 
                            ORDER BY keyword, sum(VALUE::INT) DESC
                        ) A3 
                        GROUP BY keyword
                    ) A1
                    JOIN
                    (		SELECT DISTINCT keyword, sum(VALUE::INT) sum_val, to_char(date::date,'mm/yyyy') monthly
                            FROM vn_trending
                            WHERE EXTRACT(YEAR FROM DATE) = %s
                            GROUP BY keyword,to_char(date::date,'mm/yyyy') 
                            ORDER BY keyword, sum(VALUE::INT) DESC
                        ) A2 ON A1.keyword = A2.keyword and A1.max_val = A2.sum_val
                ) B ON A.keyword = B.keyword
            ORDER BY A.sum_val DESC, A.keyword, B.monthly;""" % (year, limit, year, year)

    return sql


def main():
    key_in = input()
    try:
        if key_in.isdigit():
            int_key_in = int(key_in)
            while int_key_in != 99:
                if int_key_in == 1:
                    input_data()
                elif int_key_in == 2:
                    top_ten_trending()
                elif int_key_in == 3:
                    search_key_word()
                elif int_key_in == 4:
                    top_five_trending('2020')
                elif int_key_in == 5:
                    top_five_trending('2019')
                elif int_key_in == 6:
                    top_five_trending()
                else:
                    print("Nhập sai lựa chọn. Vui lòng chỉ nhập số.")
                print_screen()
                return main()
                # int_key_in = int(input())
            else:
                print("Bye bye!")
        else:
            print(s_help)
            return main()

    except Exception as e:
        print("Nhập sai lựa chọn." + e)
        return main()


if __name__ == '__main__':
    print_screen()
    main()
