from flask import Flask, render_template, request, flash, redirect, url_for, session
import psycopg2
from psycopg2.extras import RealDictCursor
import hashlib
import pandas as pd
from io import BytesIO

app = Flask(__name__)
app.secret_key = "super-secret-key-change-in-production"

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'my_new_project',
    'user': 'pavelbakhteev',
    'password': ''
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def get_marketplaces():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM marketplaces ORDER BY name")
    mps = cur.fetchall()
    cur.close()
    conn.close()
    return mps

@app.route("/")
def index():
    # Если пользователь авторизован — проверяем подписку
    if 'user_id' in session:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT s.tariff IS NOT NULL AS has_subscription
                FROM subscriptions s
                WHERE s.user_id = %s
            """, (session['user_id'],))
            result = cur.fetchone()
            session['has_subscription'] = bool(result['has_subscription']) if result else False
        except:
            session['has_subscription'] = False
        finally:
            cur.close()
            conn.close()

    return render_template("index.html")

@app.route("/register", methods=["GET", "POST"])
def register():
    marketplaces = get_marketplaces()
    
    if request.method == "POST":
        username = request.form.get("username")
        email = request.form.get("email")
        marketplace_id = request.form.get("marketplace_id")
        password = request.form.get("password")
        
        if not all([username, email, marketplace_id, password]):
            flash("Все поля обязательны!", "error")
            return render_template("register.html", marketplaces=marketplaces)
        
        if len(password) < 6:
            flash("Пароль должен быть не менее 6 символов", "error")
            return render_template("register.html", marketplaces=marketplaces)
        
        password_hash = hash_password(password)
        
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO users (username, email, marketplace_id, password_hash)
                VALUES (%s, %s, %s, %s) RETURNING id
            """, (username, email, marketplace_id, password_hash))
            user_id = cur.fetchone()['id']
            conn.commit()
            
            session['user_id'] = user_id
            session['email'] = email
            cur.execute("SELECT name FROM marketplaces WHERE id = %s", (marketplace_id,))
            marketplace_name = cur.fetchone()['name'].lower()
            session['marketplace'] = marketplace_name
            session['has_subscription'] = False
            
            flash("Регистрация успешна! Выберите тариф.", "success")
            return redirect(url_for("pricing"))
        except psycopg2.IntegrityError:
            conn.rollback()
            flash("Пользователь с таким username или email уже существует!", "error")
        finally:
            cur.close()
            conn.close()
    
    return render_template("register.html", marketplaces=marketplaces)

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        email = request.form.get("email")
        password = request.form.get("password")
        
        if not email or not password:
            flash("Email и пароль обязательны!", "error")
            return render_template("login.html")
        
        password_hash = hash_password(password)
        
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT u.id, u.username, m.name AS marketplace_name, s.tariff IS NOT NULL AS has_subscription
                FROM users u
                JOIN marketplaces m ON u.marketplace_id = m.id
                LEFT JOIN subscriptions s ON s.user_id = u.id
                WHERE u.email = %s AND u.password_hash = %s
            """, (email, password_hash))
            user = cur.fetchone()
            
            if user:
                session['user_id'] = user['id']
                session['username'] = user['username']
                session['email'] = email
                session['marketplace'] = user['marketplace_name'].lower()
                session['has_subscription'] = bool(user['has_subscription'])

                flash("Вход успешный!", "success")
                
                # Если есть подписка — сразу на правильную аналитику
                if user['has_subscription']:
                    if user['marketplace_name'].lower() == "wildberries":
                        return redirect(url_for("analytics_wb"))
                    else:
                        return redirect(url_for("analytics_ozon"))
                else:
                    return redirect(url_for("pricing"))
            else:
                flash("Неверный email или пароль!", "error")
        finally:
            cur.close()
            conn.close()
    
    return render_template("login.html")

@app.route("/pricing")
def pricing():
    if 'user_id' not in session:
        flash("Пожалуйста, войдите или зарегистрируйтесь", "error")
        return redirect(url_for("login"))
    return render_template("pricing.html")

@app.route("/choose_tariff/<tariff>")
def choose_tariff(tariff):
    if 'user_id' not in session:
        flash("Сессия истекла. Войдите заново.", "error")
        return redirect(url_for("login"))
    
    user_id = session['user_id']

    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Получаем маркетплейс пользователя
        cur.execute("""
            SELECT m.name AS marketplace_name
            FROM users u
            JOIN marketplaces m ON u.marketplace_id = m.id
            WHERE u.id = %s
        """, (user_id,))
        result = cur.fetchone()
        marketplace = result['marketplace_name'].lower() if result else 'wildberries'

        if tariff == "free":
            tariff_name = "free"
            price = 0
            max_reports = 3
            flash("Бесплатный тариф активирован! Лимит: 3 отчёта.", "success")
        elif tariff == "standard":
            tariff_name = "standard"
            price = 3000
            max_reports = 15
            flash("Вы выбрали тариф Стандарт (3000 ₽). Переход к оплате...", "info")
        elif tariff == "premium":
            tariff_name = "premium"
            price = 5000
            max_reports = 40
            flash("Вы выбрали тариф Премиум (5000 ₽). Переход к оплате...", "info")
        else:
            flash("Неизвестный тариф", "error")
            return redirect(url_for("pricing"))

        # Удаляем старую подписку
        cur.execute("DELETE FROM subscriptions WHERE user_id = %s", (user_id,))
        
        # Создаём новую
        cur.execute("""
            INSERT INTO subscriptions (user_id, tariff, price, reports_uploaded, max_reports)
            VALUES (%s, %s, %s, 0, %s)
        """, (user_id, tariff_name, price, max_reports))
        
        conn.commit()

        # РЕДИРЕКТ НА ПРАВИЛЬНУЮ АНАЛИТИКУ ИЛИ ОПЛАТУ
        if tariff == "free":
            if marketplace == "wildberries":
                return redirect(url_for("analytics_wb"))
            else:
                return redirect(url_for("analytics_ozon"))
        else:
            return redirect(url_for("payment", tariff=tariff))
            
    except Exception as e:
        conn.rollback()
        flash(f"Ошибка при выборе тарифа: {str(e)}", "error")
        return redirect(url_for("pricing"))
    finally:
        cur.close()
        conn.close()

@app.route("/payment/<tariff>")
def payment(tariff):
    if 'user_id' not in session:
        return redirect(url_for("login"))
    prices = {"standard": 3000, "premium": 5000}
    price = prices.get(tariff, 0)
    return render_template("payment.html", tariff=tariff.capitalize(), price=price)

@app.route("/payment_success/<tariff>")
def payment_success(tariff):
    if 'user_id' not in session:
        return redirect(url_for("login"))
    
    # После оплаты — на правильную аналитику
    user_id = session['user_id']
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT m.name AS marketplace_name
        FROM users u
        JOIN marketplaces m ON u.marketplace_id = m.id
        WHERE u.id = %s
    """, (user_id,))
    result = cur.fetchone()
    cur.close()
    conn.close()
    
    marketplace = result['marketplace_name'].lower() if result else 'wildberries'
    
    flash(f"Оплата тарифа {tariff.capitalize()} успешна! Тариф активирован.", "success")
    
    if marketplace == "wildberries":
        return redirect(url_for("analytics_wb"))
    else:
        return redirect(url_for("analytics_ozon"))

@app.route("/analytics/wb")
@app.route("/analytics/wb/<int:report_id>")
def analytics_wb(report_id=None):
    if 'user_id' not in session:
        return redirect(url_for("login"))
    
    session['current_marketplace'] = 'wb'
    email = session.get('email', 'Неизвестно')
    
    # Если передан report_id в URL - загружаем его
    if report_id is None:
        # Если нет report_id в URL, берем из сессии
        report_id = session.get('current_report_id')
    
    report_data = None
    
    if report_id:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            # Получаем данные отчета (включая k_perenum)
            cur.execute("""
                SELECT id, report_period, revenue, logistics, storage, 
                       other_deductions, itogo_k_oplate, k_perenum, tax_amount, net_profit,
                       start_date, end_date
                FROM user_reports 
                WHERE id = %s AND user_id = %s
            """, (report_id, session['user_id']))
            report = cur.fetchone()
            
            if report:
                # Получаем артикулы
                cur.execute("""
                    SELECT article, name, quantity, revenue as article_revenue, cost
                    FROM user_products 
                    WHERE report_id = %s
                    ORDER BY article
                """, (report_id,))
                articles = cur.fetchall()
                
                articles_data = []
                for a in articles:
                    articles_data.append({
                        'article': a['article'],
                        'name': a['name'],
                        'quantity': a['quantity'],
                        'revenue': float(a['article_revenue']) if a['article_revenue'] else 0,
                        'cost': float(a['cost']) if a['cost'] else 0
                    })
                
                # k_perenum берем из отдельного поля в БД
                k_perenum = float(report['k_perenum']) if report['k_perenum'] else 0
                
                report_data = {
                    'period': report['report_period'],
                    'revenue': float(report['revenue']) if report['revenue'] else 0,
                    'k_perenum': k_perenum,
                    'logistics': float(report['logistics']) if report['logistics'] else 0,
                    'storage': float(report['storage']) if report['storage'] else 0,
                    'other_deductions': float(report['other_deductions']) if report['other_deductions'] else 0,
                    'itogo_k_oplate': float(report['itogo_k_oplate']) if report['itogo_k_oplate'] else 0,
                    'tax_amount': float(report['tax_amount']) if report['tax_amount'] else 0,
                    'net_profit': float(report['net_profit']) if report['net_profit'] else 0,
                    'articles': articles_data,
                    'report_id': report['id']
                }
                
                # Обновляем сессию текущим report_id
                session['current_report_id'] = report_id
                
        except Exception as e:
            print(f"Ошибка при загрузке отчета из БД: {e}")
        finally:
            cur.close()
            conn.close()
    
    return render_template("analytics_wb.html", 
                         user_email=email, 
                         analysis_result=report_data)

import pandas as pd
from io import BytesIO

def save_user_report(user_id, articles_data, period, analysis_result, report_type='main'):
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        start_date = None
        end_date = None
        if period and ' – ' in period:
            parts = period.split(' – ')
            try:
                start_date = pd.to_datetime(parts[0], format='%d.%m.%Y').date()
                end_date = pd.to_datetime(parts[1], format='%d.%m.%Y').date()
            except:
                pass
        
        cur.execute("""
            INSERT INTO user_reports (
                user_id, report_period, start_date, end_date,
                revenue, logistics, storage, other_deductions, itogo_k_oplate, k_perenum, report_type
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (user_id, period, start_date, end_date,
              analysis_result.get('revenue', 0),
              analysis_result.get('logistics', 0),
              analysis_result.get('storage', 0),
              analysis_result.get('other_deductions', 0),
              analysis_result.get('itogo_k_oplate', 0),
              analysis_result.get('k_perenum', 0),  # ДОБАВЛЕНО!
              report_type))
        
        report_id = cur.fetchone()['id']
        print(f"Создан новый отчет {period} с ID: {report_id}, тип: {report_type}")
        
        for article in articles_data:
            cur.execute("""
                INSERT INTO user_products (report_id, article, name, quantity, revenue, cost)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (report_id, 
                  article.get('article'),
                  article.get('name'),
                  article.get('quantity', 0),
                  article.get('revenue', 0),
                  0))
        
        conn.commit()
        return report_id
        
    except Exception as e:
        conn.rollback()
        print(f"Ошибка при сохранении отчета: {e}")
        raise
    finally:
        cur.close()
        conn.close()

def update_report_pl(report_id, tax_rate, other_deductions):
    """
    Обновляет P&L показатели отчета:
    - other_deductions сохраняется в БД
    - tax_amount = itogo_k_oplate * tax_rate / 100
    - net_profit = itogo_k_oplate - tax_amount - total_cogs - other_deductions
    - total_cogs считается из user_products
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Получаем itogo_k_oplate из отчета
        cur.execute("""
            SELECT itogo_k_oplate FROM user_reports WHERE id = %s
        """, (report_id,))
        report = cur.fetchone()
        
        if not report:
            return None
        
        itogo_k_oplate = float(report['itogo_k_oplate']) if report['itogo_k_oplate'] else 0
        
        # Считаем общую себестоимость по всем артикулам этого отчета
        cur.execute("""
            SELECT SUM(cost * quantity) as total_cogs
            FROM user_products up
            WHERE up.report_id = %s
        """, (report_id,))
        result = cur.fetchone()
        total_cogs = float(result['total_cogs']) if result and result['total_cogs'] else 0
        
        # Рассчитываем налог
        tax_amount = itogo_k_oplate * (tax_rate / 100)
        
        # Рассчитываем чистую прибыль
        net_profit = itogo_k_oplate - tax_amount - total_cogs - other_deductions
        
        # Обновляем отчет - ДОБАВЛЯЕМ other_deductions
        cur.execute("""
            UPDATE user_reports 
            SET tax_amount = %s,
                other_deductions = %s,
                net_profit = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (tax_amount, other_deductions, net_profit, report_id))
        
        conn.commit()
        print(f"Обновлен P&L для отчета {report_id}: налог={tax_amount}, прочие={other_deductions}, себестоимость={total_cogs}, чистая прибыль={net_profit}")
        
        return {
            'tax_amount': tax_amount,
            'other_deductions': other_deductions,
            'total_cogs': total_cogs,
            'net_profit': net_profit
        }
        
    except Exception as e:
        conn.rollback()
        print(f"Ошибка при обновлении P&L: {e}")
        raise
    finally:
        cur.close()
        conn.close()

@app.route("/save_costs", methods=["POST"])
def save_costs():
    if 'user_id' not in session:
        return {"success": False, "error": "Not logged in"}, 401
    
    data = request.get_json()
    articles = data.get('articles', [])
    report_id = data.get('report_id') or session.get('current_report_id')
    tax_rate = float(data.get('tax_rate', 0))
    other_deductions = float(data.get('other_deductions', 0))
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Обновляем себестоимость для каждого артикула
        for item in articles:
            article = item.get('article')
            cost = item.get('cost', 0)
            
            # Обновляем себестоимость в user_products
            cur.execute("""
                UPDATE user_products 
                SET cost = %s, updated_at = CURRENT_TIMESTAMP
                WHERE report_id = %s AND article = %s
            """, (cost, report_id, article))
        
        conn.commit()
        print(f"Обновлено {len(articles)} артикулов")
        
        # После обновления себестоимости пересчитываем P&L
        pl_data = update_report_pl(report_id, tax_rate, other_deductions)
        
        return {
            "success": True, 
            "message": f"Сохранено {len(articles)} себестоимостей",
            "pl_data": pl_data
        }
        
    except Exception as e:
        conn.rollback()
        print(f"Ошибка при сохранении себестоимости: {e}")
        return {"success": False, "error": str(e)}, 500
    finally:
        cur.close()
        conn.close()

@app.route("/get_saved_costs", methods=["POST"])
def get_saved_costs():
    if 'user_id' not in session:
        return {"success": False, "error": "Not logged in"}, 401
    
    data = request.get_json()
    articles = data.get('articles', [])
    
    if not articles:
        return {"success": True, "costs": {}}
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        result = {}
        for article in articles:
            cur.execute("""
                SELECT cost FROM user_products 
                WHERE article = %s 
                AND report_id IN (SELECT id FROM user_reports WHERE user_id = %s)
                AND cost > 0
                ORDER BY created_at DESC LIMIT 1
            """, (article, session['user_id']))
            row = cur.fetchone()
            if row and row['cost']:
                result[article] = float(row['cost'])
        
        return {"success": True, "costs": result}
        
    except Exception as e:
        print(f"Ошибка: {e}")
        return {"success": False, "error": str(e)}, 500
    finally:
        cur.close()
        conn.close()

@app.route("/analyze_wb", methods=["POST"])
def analyze_wb():
    if 'user_id' not in session:
        return redirect(url_for("login"))
    
    report_files = request.files.getlist('report_files')
    
    if not report_files or len(report_files) == 0:
        flash("Пожалуйста, загрузите файл(ы) формата .xlsx", "error")
        return redirect(url_for("analytics_wb"))
    
    for file in report_files:
        if not file.filename.endswith('.xlsx'):
            flash(f"Файл {file.filename} не является .xlsx", "error")
            return redirect(url_for("analytics_wb"))
    
    # Если загружен 1 файл — возвращаем JSON
    if len(report_files) == 1:
        result = process_single_report(report_files[0])
        # Важно: возвращаем JSON, а не redirect
        return result
    
    # Если загружено 2 файла — объединяем
    elif len(report_files) == 2:
        result = process_merged_reports(report_files[0], report_files[1])
        if result.get('redirect'):
            return redirect(result['redirect'])
        return result
    
    else:
        flash("Можно загрузить не более 2 файлов за раз", "error")
        return redirect(url_for("analytics_wb"))
def process_single_report(file):
    """Обработка одного файла с выбором типа отчета"""
    try:
        # Сначала парсим данные
        df = pd.read_excel(BytesIO(file.read()))
        result = parse_wb_report(df)
        
        # Возвращаем JSON
        return {
            'need_type_selection': True,
            'result': {
                'period': result['period'],
                'revenue': result['analysis_result']['revenue'],
                'k_perenum': result['analysis_result']['k_perenum'],
                'logistics': result['analysis_result']['logistics'],
                'storage': result['analysis_result']['storage'],
                'other_deductions': result['analysis_result']['other_deductions'],
                'itogo_k_oplate': result['analysis_result']['itogo_k_oplate'],
                'articles': result['articles_data']
            }
        }
        
    except Exception as e:
        print(f"Ошибка при анализе файла: {e}")
        return {"error": str(e)}
    
def process_merged_reports(file1, file2):
    """Объединение двух отчётов за одну неделю"""
    try:
        # Читаем оба файла
        df1 = pd.read_excel(BytesIO(file1.read()))
        df2 = pd.read_excel(BytesIO(file2.read()))
        
        # Определяем тип каждого отчёта
        type1 = detect_report_type(df1)
        type2 = detect_report_type(df2)
        
        # Определяем, какой основной, а какой по выкупам
        if type1 == 'main':
            main_df = df1
            buyout_df = df2
        elif type2 == 'main':
            main_df = df2
            buyout_df = df1
        else:
            # Если оба не основные — пробуем объединить как есть
            main_df = df1
            buyout_df = df2
        
        # Парсим основной отчёт
        main_result = parse_wb_report(main_df)
        
        # Парсим отчёт по выкупам (только продажи и выручка)
        buyout_result = parse_buyout_report(buyout_df)
        
        # Объединяем данные
        merged_result = merge_reports(main_result, buyout_result)
        
        # Сохраняем объединённый отчёт (тип 'merged')
        report_id = save_user_report(
            session['user_id'], 
            merged_result['articles_data'], 
            merged_result['period'], 
            merged_result['analysis_result'],
            'merged'
        )
        
        session['current_report_id'] = report_id
        
        # Возвращаем JSON с redirect
        return {"redirect": url_for('analytics_wb', report_id=report_id)}
        
    except Exception as e:
        print(f"Ошибка при объединении отчётов: {e}")
        return {"error": str(e)}
    
def detect_report_type(df):
    """Определяет тип отчёта: 'main' (основной) или 'buyout' (по выкупам)"""
    # Проверяем первые строки датафрейма
    first_rows = df.head(10).to_string()
    
    # Признаки основного отчёта
    if 'По выкупам' in first_rows:
        return 'buyout'
    else:
        return 'main'
def parse_buyout_report(df):
    """Парсинг отчёта 'По выкупам' — используем столбец P"""
    
    sales_mask = df['Тип документа'] == 'Продажа'
    sales_df = df[sales_mask]
    
    # Считаем k_perenum из столбца
    k_perenum_column = 'К перечислению Продавцу за реализованный Товар'
    k_perenum = 0
    if k_perenum_column in df.columns:
        k_perenum = df.loc[sales_mask, k_perenum_column].sum()
    
    # Считаем логистику
    logistics_column = 'Услуги по доставке товара покупателю'
    logistics = df[logistics_column].abs().sum() if logistics_column in df.columns else 0
    
    # Итого к оплате
    itogo_k_oplate = k_perenum - logistics
    
    articles_data = []
    
    if not sales_df.empty:
        # Используем правильный столбец
        grouped = sales_df.groupby(['Артикул поставщика', 'Название']).agg({
            'Кол-во': 'sum',
            'Вайлдберриз реализовал Товар (Пр)': 'sum'  # ← ЭТО ГЛАВНОЕ
        }).reset_index()
        
        for _, row in grouped.iterrows():
            articles_data.append({
                'article': str(row['Артикул поставщика']),
                'name': row['Название'],
                'quantity': int(row['Кол-во']),
                'revenue': float(row['Вайлдберриз реализовал Товар (Пр)']),  # ← столбец P
                'cost': 0
            })
    
    total_revenue = sum(a['revenue'] for a in articles_data)
    
    return {
        'articles_data': articles_data,
        'total_revenue': total_revenue,
        'k_perenum': float(k_perenum),      # ← ДОБАВИТЬ
        'itogo_k_oplate': float(itogo_k_oplate),  # ← ДОБАВИТЬ
        'logistics': float(logistics)        # ← ДОБАВИТЬ
    }
def merge_reports(main_result, buyout_result):
    """Объединяет основной отчёт и отчёт по выкупам"""
    
    # 1. Суммируем финансовые показатели
    analysis_result = {
        'revenue': main_result['analysis_result']['revenue'] + buyout_result['total_revenue'],
        'k_perenum': main_result['analysis_result']['k_perenum'] + buyout_result.get('k_perenum', 0),
        'logistics': main_result['analysis_result']['logistics'],
        'storage': main_result['analysis_result']['storage'],
        'other_deductions': main_result['analysis_result']['other_deductions'],
        'fines': main_result['analysis_result'].get('fines', 0),
        'correction': main_result['analysis_result'].get('correction', 0),
        'itogo_k_oplate': main_result['analysis_result']['itogo_k_oplate'] + buyout_result.get('itogo_k_oplate', 0)
    }
    
    # 2. Объединяем артикулы
    articles_dict = {}
    
    # Добавляем артикулы из основного отчёта
    for article in main_result['articles_data']:
        art_key = str(article['article'])
        articles_dict[art_key] = {
            'article': art_key,
            'name': article['name'],
            'quantity': article['quantity'],
            'revenue': article['revenue'],
            'cost': 0
        }
    
    # Добавляем/суммируем артикулы из отчёта по выкупам
    for article in buyout_result['articles_data']:
        art_key = str(article['article'])
        if art_key in articles_dict:
            # Артикул уже есть — суммируем
            articles_dict[art_key]['quantity'] += article['quantity']
            articles_dict[art_key]['revenue'] += article['revenue']
        else:
            # Новый артикул — добавляем
            articles_dict[art_key] = {
                'article': art_key,
                'name': article['name'],
                'quantity': article['quantity'],
                'revenue': article['revenue'],
                'cost': 0
            }
    
    # Преобразуем обратно в список
    merged_articles = list(articles_dict.values())
    
    # Период оставляем из основного отчёта
    period = main_result['period']
    
    return {
        'period': period,
        'analysis_result': analysis_result,
        'articles_data': merged_articles
    }
def parse_wb_report(df):
    """Парсинг стандартного отчёта Wildberries (существующая логика)"""
    # 1. "Продажа" = Продажи по столбцу «Вайлдберриз реализовал товар» - Возвраты
    wb_realized_column = 'Вайлдберриз реализовал Товар (Пр)'
    
    sales_mask = df['Тип документа'] == 'Продажа'
    sales_wb_realized = df.loc[sales_mask, wb_realized_column].sum() if wb_realized_column in df.columns else 0
    
    returns_mask = df['Тип документа'] == 'Возврат'
    returns_wb_realized = df.loc[returns_mask, wb_realized_column].sum() if wb_realized_column in df.columns else 0
    
    revenue = sales_wb_realized - returns_wb_realized

    # 2. "К перечислению за товар"
    k_perenum_column = 'К перечислению Продавцу за реализованный Товар'
    if k_perenum_column in df.columns:
        sales_k_perenum = df.loc[sales_mask, k_perenum_column].sum()
        returns_k_perenum = df.loc[returns_mask, k_perenum_column].sum()
        k_perenum = sales_k_perenum - returns_k_perenum
    else:
        k_perenum = 0

    # 3. Логистика
    logistics_column = 'Услуги по доставке товара покупателю'
    logistics = df[logistics_column].abs().sum() if logistics_column in df.columns else 0

    # 4. Хранение
    storage_column = 'Хранение'
    storage = df[storage_column].abs().sum() if storage_column in df.columns else 0

    # 5. Прочие удержания
    other_rows = df[df['Тип документа'] == 'Удержание']
    other_deductions = 0
    if 'Удержания' in df.columns:
        other_deductions = other_rows['Удержания'].abs().sum()
    
    # 6. Штрафы
    fines_column = 'Общая сумма штрафов'
    fines = df[fines_column].abs().sum() if fines_column in df.columns else 0

    # 7. Корректировка ВВ
    correction_column = 'Корректировка Вознаграждения Вайлдберриз (ВВ)'
    correction = df[correction_column].sum() if correction_column in df.columns else 0

    # Итого к оплате
    itogo_k_oplate = k_perenum - logistics - storage - other_deductions - abs(fines) - abs(correction)
    
    # Группировка по артикулам (только продажи)
    sales_df = df[sales_mask]
    articles_data = []
    
    if not sales_df.empty:
        grouped = sales_df.groupby(['Артикул поставщика', 'Название']).agg({
            'Кол-во': 'sum',
            'Цена розничная с учетом согласованной скидки': 'sum'
        }).reset_index()
        
        for _, row in grouped.iterrows():
            articles_data.append({
                'article': str(row['Артикул поставщика']),
                'name': row['Название'],
                'quantity': int(row['Кол-во']),
                'revenue': float(row['Цена розничная с учетом согласованной скидки']),
                'cost': 0
            })
    
    # Период
    df['Дата продажи'] = pd.to_datetime(df['Дата продажи'], errors='coerce')
    date_min = df['Дата продажи'].min()
    date_max = df['Дата продажи'].max()
    period = "Период не определён"
    if pd.notna(date_min) and pd.notna(date_max):
        period = f"{date_min.strftime('%d.%m.%Y')} – {date_max.strftime('%d.%m.%Y')}"
    
    analysis_result = {
        'revenue': float(revenue),
        'k_perenum': float(k_perenum),
        'logistics': float(logistics),
        'storage': float(storage),
        'other_deductions': float(other_deductions),
        'fines': float(fines),
        'correction': float(correction),
        'itogo_k_oplate': float(itogo_k_oplate)
    }
    
    return {
        'period': period,
        'analysis_result': analysis_result,
        'articles_data': articles_data
    }

@app.route("/save_with_type", methods=["POST"])
def save_with_type():
    if 'user_id' not in session:
        return redirect(url_for("login"))
    
    data = request.get_json()
    report_type = data.get('report_type', 'main')
    period = data.get('period')
    revenue = data.get('revenue')
    k_perenum = data.get('k_perenum')
    logistics = data.get('logistics')
    storage = data.get('storage')
    other_deductions = data.get('other_deductions')
    itogo_k_oplate = data.get('itogo_k_oplate')
    articles = data.get('articles')
    
    analysis_result = {
        'revenue': revenue,
        'k_perenum': k_perenum,
        'logistics': logistics,
        'storage': storage,
        'other_deductions': other_deductions,
        'itogo_k_oplate': itogo_k_oplate
    }
    
    report_id = save_user_report(
        session['user_id'], 
        articles, 
        period, 
        analysis_result,
        report_type
    )
    
    session['current_report_id'] = report_id
    
    return {"success": True, "report_id": report_id, "report_type": report_type}

@app.route("/save_pl", methods=["POST"])
def save_pl():
    if 'user_id' not in session:
        return {"success": False, "error": "Not logged in"}, 401
    
    data = request.get_json()
    tax_rate = float(data.get('tax_rate', 0))
    other_deductions = float(data.get('other_deductions', 0))
    report_id = data.get('report_id')
    
    if not report_id:
        # Если нет report_id, берем последний отчет пользователя
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id FROM user_reports 
            WHERE user_id = %s 
            ORDER BY created_at DESC LIMIT 1
        """, (session['user_id'],))
        result = cur.fetchone()
        cur.close()
        conn.close()
        
        if result:
            report_id = result['id']
        else:
            return {"success": False, "error": "No report found"}, 404
    
    pl_data = update_report_pl(report_id, tax_rate, other_deductions)
    
    if pl_data:
        return {
            "success": True, 
            "message": "P&L данные сохранены",
            "pl_data": pl_data
        }
    else:
        return {"success": False, "error": "Failed to update P&L"}, 500
    
def save_user_articles(user_id, articles, report_period):
    """
    Сохраняет отчет и артикулы пользователя
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Сначала сохраняем отчет
        cur.execute("""
            INSERT INTO user_reports (user_id, report_period)
            VALUES (%s, %s)
            RETURNING id
        """, (user_id, report_period))
        
        report_id = cur.fetchone()['id']
        
        # Сохраняем артикулы для этого отчета
        for article in articles:
            cur.execute("""
                INSERT INTO user_products (report_id, article, name)
                VALUES (%s, %s, %s)
            """, (report_id, article['article'], article['name']))
        
        conn.commit()
        print(f"Сохранен отчет с периодом {report_period} и {len(articles)} артикулами для пользователя {user_id}")
        
    except Exception as e:
        conn.rollback()
        print(f"Ошибка при сохранении отчета: {e}")
        raise
    finally:
        cur.close()
        conn.close()
    
@app.route('/unit-economics')
def unit_economics():
    if 'user_id' not in session:
        return redirect(url_for("login"))
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Получаем все артикулы пользователя с их unit-данными
        cur.execute("""
            SELECT DISTINCT ON (up.article) 
                up.article, 
                up.name,
                up.cost,
                up.advertising,
                up.logistics_unit as logistics,
                up.storage_unit as storage,
                up.commission,
                up.buyout_percent,
                up.tax_percent,
                up.price,
                up.discount,
                up.spp_percent,
                ur.report_period,
                ur.created_at as report_date
            FROM user_products up
            JOIN user_reports ur ON up.report_id = ur.id
            WHERE ur.user_id = %s
            ORDER BY up.article, ur.created_at DESC
        """, (session['user_id'],))
        
        articles = cur.fetchall()
        
        # Преобразуем None в 0 для всех полей
        articles_list = []
        for a in articles:
            articles_list.append({
                'article': a['article'],
                'name': a['name'],
                'cost': float(a['cost']) if a['cost'] else 0,
                'advertising': float(a['advertising']) if a['advertising'] else 0,
                'logistics': float(a['logistics']) if a['logistics'] else 0,
                'storage': float(a['storage']) if a['storage'] else 0,
                'commission': float(a['commission']) if a['commission'] else 0,
                'buyout_percent': float(a['buyout_percent']) if a['buyout_percent'] else 0,
                'tax_percent': float(a['tax_percent']) if a['tax_percent'] else 0,
                'price': float(a['price']) if a['price'] else 0,
                'discount': float(a['discount']) if a['discount'] else 0,
                'spp_percent': float(a['spp_percent']) if a['spp_percent'] else 0
            })
        
        print(f"Найдено артикулов: {len(articles_list)}")
        
    except Exception as e:
        print(f"Ошибка при получении артикулов: {e}")
        articles_list = []
    finally:
        cur.close()
        conn.close()
    
    return render_template('unit_economics.html', 
                         user_email=session.get('email', 'Неизвестно'),
                         articles=articles_list)

@app.route("/save_unit_data", methods=["POST"])
def save_unit_data():
    if 'user_id' not in session:
        return {"success": False, "error": "Not logged in"}, 401
    
    data = request.get_json()
    unit_data = data.get('unit_data', [])
    
    if not unit_data:
        return {"success": False, "error": "No data provided"}, 400
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        for item in unit_data:
            article = item.get('article')
            
            cur.execute("""
                UPDATE user_products 
                SET advertising = %s,
                    logistics_unit = %s,
                    storage_unit = %s,
                    commission = %s,
                    buyout_percent = %s,
                    tax_percent = %s,
                    price = %s,
                    discount = %s,
                    spp_percent = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE report_id IN (
                    SELECT id FROM user_reports WHERE user_id = %s
                )
                AND article = %s
            """, (
                item.get('advertising', 0),
                item.get('logistics', 0),
                item.get('storage', 0),
                item.get('commission', 0),
                item.get('buyout_percent', 0),
                item.get('tax_percent', 0),
                item.get('price', 0),
                item.get('discount', 0),
                item.get('spp_percent', 0),
                session['user_id'],
                article
            ))
        
        conn.commit()
        return {"success": True, "message": f"Сохранено {len(unit_data)} товаров"}
        
    except Exception as e:
        conn.rollback()
        print(f"Ошибка при сохранении unit-данных: {e}")
        return {"success": False, "error": str(e)}, 500
    finally:
        cur.close()
        conn.close()

@app.route('/pl')
def pl():
    if 'user_id' not in session:
        return redirect(url_for("login"))
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Получаем все отчеты пользователя, включая report_type
        cur.execute("""
            SELECT id, report_period, start_date, end_date, 
                   revenue, logistics, storage, other_deductions, 
                   itogo_k_oplate, tax_amount, net_profit, created_at, report_type
            FROM user_reports 
            WHERE user_id = %s 
            ORDER BY created_at DESC
        """, (session['user_id'],))
        
        reports = cur.fetchall()
        
        # Для каждого отчета получаем количество артикулов и общую выручку по артикулам
        for report in reports:
            cur.execute("""
                SELECT COUNT(*) as article_count, SUM(revenue) as articles_total_revenue
                FROM user_products 
                WHERE report_id = %s
            """, (report['id'],))
            result = cur.fetchone()
            report['article_count'] = result['article_count'] if result else 0
            report['articles_total_revenue'] = float(result['articles_total_revenue']) if result and result['articles_total_revenue'] else 0
            
        print(f"Найдено отчетов: {len(reports)}")
        
    except Exception as e:
        print(f"Ошибка при получении отчетов: {e}")
        reports = []
    finally:
        cur.close()
        conn.close()
    
    return render_template('pl.html', 
                         user_email=session.get('email', 'Неизвестно'),
                         reports=reports)

@app.route('/pl/filter', methods=['POST'])
def pl_filter():
    if 'user_id' not in session:
        return {"success": False, "error": "Not logged in"}, 401
    
    data = request.get_json()
    report_ids = data.get('report_ids', [])
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        if report_ids:
            placeholders = ','.join(['%s'] * len(report_ids))
            cur.execute(f"""
                SELECT id, report_period, start_date, end_date, 
                       revenue, logistics, storage, other_deductions, 
                       itogo_k_oplate, tax_amount, net_profit, created_at, report_type
                FROM user_reports 
                WHERE user_id = %s AND id IN ({placeholders})
                ORDER BY created_at DESC
            """, [session['user_id']] + report_ids)
        else:
            cur.execute("""
                SELECT id, report_period, start_date, end_date, 
                       revenue, logistics, storage, other_deductions, 
                       itogo_k_oplate, tax_amount, net_profit, created_at, report_type
                FROM user_reports 
                WHERE user_id = %s 
                ORDER BY created_at DESC
            """, (session['user_id'],))
        
        reports = cur.fetchall()
        
        reports_data = []
        for report in reports:
            # Получаем общую выручку по артикулам
            cur.execute("""
                SELECT SUM(revenue) as articles_total_revenue
                FROM user_products 
                WHERE report_id = %s
            """, (report['id'],))
            result = cur.fetchone()
            articles_total_revenue = float(result['articles_total_revenue']) if result and result['articles_total_revenue'] else 0
            
            cur.execute("""
                SELECT COUNT(*) as article_count
                FROM user_products 
                WHERE report_id = %s
            """, (report['id'],))
            count_result = cur.fetchone()
            article_count = count_result['article_count'] if count_result else 0
            
            reports_data.append({
                'id': report['id'],
                'report_period': report['report_period'],
                'start_date': report['start_date'].isoformat() if report['start_date'] else None,
                'end_date': report['end_date'].isoformat() if report['end_date'] else None,
                'revenue': float(report['revenue']) if report['revenue'] else 0,
                'articles_total_revenue': articles_total_revenue,
                'article_count': article_count,
                'logistics': float(report['logistics']) if report['logistics'] else 0,
                'storage': float(report['storage']) if report['storage'] else 0,
                'other_deductions': float(report['other_deductions']) if report['other_deductions'] else 0,
                'itogo_k_oplate': float(report['itogo_k_oplate']) if report['itogo_k_oplate'] else 0,
                'tax_amount': float(report['tax_amount']) if report['tax_amount'] else 0,
                'net_profit': float(report['net_profit']) if report['net_profit'] else 0,
                'created_at': report['created_at'].isoformat() if report['created_at'] else None,
                'report_type': report['report_type'] if report['report_type'] else 'main'
            })
        
        return {"success": True, "reports": reports_data}
        
    except Exception as e:
        print(f"Ошибка при фильтрации отчетов: {e}")
        return {"success": False, "error": str(e)}, 500
    finally:
        cur.close()
        conn.close()
        
@app.route("/delete_articles", methods=["POST"])
def delete_articles():
    if 'user_id' not in session:
        return {"success": False, "error": "Not logged in"}, 401
    
    data = request.get_json()
    articles = data.get('articles', [])
    
    if not articles:
        return {"success": False, "error": "No articles selected"}, 400
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Удаляем выбранные артикулы из всех отчетов пользователя
        for article in articles:
            cur.execute("""
                DELETE FROM user_products 
                WHERE article = %s 
                AND report_id IN (
                    SELECT id FROM user_reports WHERE user_id = %s
                )
            """, (article, session['user_id']))
        
        conn.commit()
        deleted_count = len(articles)
        print(f"Удалено {deleted_count} артикулов")
        
        return {
            "success": True, 
            "message": f"Удалено {deleted_count} товаров"
        }
        
    except Exception as e:
        conn.rollback()
        print(f"Ошибка при удалении артикулов: {e}")
        return {"success": False, "error": str(e)}, 500
    finally:
        cur.close()
        conn.close()

@app.route("/add_article", methods=["POST"])
def add_article():
    if 'user_id' not in session:
        return {"success": False, "error": "Not logged in"}, 401
    
    data = request.get_json()
    article = data.get('article', '').strip()
    name = data.get('name', '').strip()
    cost = float(data.get('cost', 0))
    
    if not article:
        return {"success": False, "error": "Артикул обязателен"}, 400
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Получаем последний отчет пользователя
        cur.execute("""
            SELECT id FROM user_reports 
            WHERE user_id = %s 
            ORDER BY created_at DESC LIMIT 1
        """, (session['user_id'],))
        report = cur.fetchone()
        
        if not report:
            return {"success": False, "error": "Сначала загрузите отчет"}, 400
        
        report_id = report['id']
        
        # Проверяем, существует ли уже такой артикул в этом отчете
        cur.execute("""
            SELECT id FROM user_products 
            WHERE report_id = %s AND article = %s
        """, (report_id, article))
        existing = cur.fetchone()
        
        if existing:
            return {"success": False, "error": "Товар с таким артикулом уже существует"}, 400
        
        # Добавляем новый товар
        cur.execute("""
            INSERT INTO user_products (report_id, article, name, cost, quantity, revenue)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (report_id, article, name, cost, 0, 0))
        
        conn.commit()
        
        return {
            "success": True,
            "message": "Товар добавлен",
            "article": {
                "article": article,
                "name": name,
                "cost": cost
            }
        }
        
    except Exception as e:
        conn.rollback()
        print(f"Ошибка при добавлении товара: {e}")
        return {"success": False, "error": str(e)}, 500
    finally:
        cur.close()
        conn.close()

@app.route("/get_report/<int:report_id>")
def get_report(report_id):
    if 'user_id' not in session:
        return {"success": False, "error": "Not logged in"}, 401
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Получаем данные отчета
        cur.execute("""
            SELECT id, report_period, revenue, logistics, storage, 
                   other_deductions, itogo_k_oplate, tax_amount, net_profit,
                   start_date, end_date
            FROM user_reports 
            WHERE id = %s AND user_id = %s
        """, (report_id, session['user_id']))
        report = cur.fetchone()
        
        if not report:
            return {"success": False, "error": "Report not found"}, 404
        
        # Получаем артикулы отчета
        cur.execute("""
            SELECT article, name, quantity, revenue as article_revenue, cost
            FROM user_products 
            WHERE report_id = %s
            ORDER BY article
        """, (report_id,))
        articles = cur.fetchall()
        
        # Форматируем данные для отправки
        articles_data = []
        for a in articles:
            articles_data.append({
                'article': a['article'],
                'name': a['name'],
                'quantity': a['quantity'],
                'revenue': float(a['article_revenue']) if a['article_revenue'] else 0,
                'cost': float(a['cost']) if a['cost'] else 0
            })
        
        return {
            "success": True,
            "report": {
                "id": report['id'],
                "period": report['report_period'],
                "revenue": float(report['revenue']) if report['revenue'] else 0,
                "logistics": float(report['logistics']) if report['logistics'] else 0,
                "storage": float(report['storage']) if report['storage'] else 0,
                "other_deductions": float(report['other_deductions']) if report['other_deductions'] else 0,
                "itogo_k_oplate": float(report['itogo_k_oplate']) if report['itogo_k_oplate'] else 0,
                "tax_amount": float(report['tax_amount']) if report['tax_amount'] else 0,
                "net_profit": float(report['net_profit']) if report['net_profit'] else 0
            },
            "articles": articles_data
        }
        
    except Exception as e:
        print(f"Ошибка при получении отчета: {e}")
        return {"success": False, "error": str(e)}, 500
    finally:
        cur.close()
        conn.close()

@app.route("/delete_report/<int:report_id>", methods=["DELETE"])
def delete_report(report_id):
    if 'user_id' not in session:
        return {"success": False, "error": "Not logged in"}, 401
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Проверяем, что отчет принадлежит пользователю
        cur.execute("""
            SELECT id FROM user_reports 
            WHERE id = %s AND user_id = %s
        """, (report_id, session['user_id']))
        report = cur.fetchone()
        
        if not report:
            return {"success": False, "error": "Report not found"}, 404
        
        # Удаляем отчет (артикулы удалятся автоматически из-за ON DELETE CASCADE)
        cur.execute("DELETE FROM user_reports WHERE id = %s", (report_id,))
        conn.commit()
        
        print(f"Отчет {report_id} удален пользователем {session['user_id']}")
        
        return {"success": True, "message": "Отчет удален"}
        
    except Exception as e:
        conn.rollback()
        print(f"Ошибка при удалении отчета: {e}")
        return {"success": False, "error": str(e)}, 500
    finally:
        cur.close()
        conn.close()
        
@app.route("/logout")
def logout():
    session.clear()
    flash("Вы вышли из аккаунта", "info")
    return redirect(url_for("index"))

@app.route("/about")
def about():
    if 'user_id' in session:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT 1 FROM subscriptions WHERE user_id = %s AND tariff IS NOT NULL LIMIT 1", (session['user_id'],))
            session['has_subscription'] = cur.fetchone() is not None
        except:
            session['has_subscription'] = False
        finally:
            cur.close()
            conn.close()
    return render_template("about.html")

@app.route("/contacts")
def contacts():
    if 'user_id' in session:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM subscriptions 
                    WHERE user_id = %s AND tariff IS NOT NULL
                ) AS has_subscription
            """, (session['user_id'],))
            result = cur.fetchone()
            session['has_subscription'] = bool(result['has_subscription']) if result else False
        except Exception as e:
            print(f"Error checking subscription: {e}")
            session['has_subscription'] = False
        finally:
            cur.close()
            conn.close()
    return render_template("contacts.html")

@app.route("/submit_feedback", methods=["POST"])
def submit_feedback():
    name = request.form.get("name")
    phone = request.form.get("phone")
    reason = request.form.get("reason")
    other_reason = request.form.get("other_reason")  # может быть пустым

    if not name or not phone or not reason:
        flash("Заполните все обязательные поля", "error")
        return redirect(url_for("contacts"))
    
    if reason == "Другое" and not other_reason:
        flash("Опишите подробнее причину обращения", "error")
        return redirect(url_for("contacts"))
    
    # Очищаем телефон
    phone_clean = ''.join(filter(str.isdigit, phone))
    if len(phone_clean) < 10:
        flash("Введите корректный номер телефона", "error")
        return redirect(url_for("contacts"))
    
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO feedback (name, phone, reason, other_reason)
            VALUES (%s, %s, %s, %s)
        """, (name.strip(), phone_clean, reason, other_reason.strip() if other_reason else None))
        conn.commit()
        flash("Спасибо за обращение! Мы свяжемся с вами в ближайшее время.", "success")
    except Exception as e:
        conn.rollback()
        flash("Ошибка при отправке. Попробуйте позже.", "error")
    finally:
        cur.close()
        conn.close()
    
    return redirect(url_for("contacts"))

@app.route("/pricing2")
def pricing2():
    if 'user_id' in session:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM subscriptions 
                    WHERE user_id = %s AND tariff IS NOT NULL
                ) AS has_subscription
            """, (session['user_id'],))
            result = cur.fetchone()
            session['has_subscription'] = bool(result['has_subscription']) if result else False
        except Exception as e:
            print(f"Error checking subscription: {e}")
            session['has_subscription'] = False
        finally:
            cur.close()
            conn.close()
    return render_template("pricing2.html")

if __name__ == "__main__":
    print("Сервер запущен: http://127.0.0.1:5000")
    app.run(debug=True, port=5000)