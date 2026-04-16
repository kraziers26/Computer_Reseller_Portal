import os
from flask import Flask
from flask_login import LoginManager
from .db import get_db
from .models import User

login_manager = LoginManager()


def create_app():
    app = Flask(__name__, template_folder='templates', static_folder='static')
    app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-change-in-prod')

    login_manager.init_app(app)
    login_manager.login_view    = 'auth.login'
    login_manager.login_message = 'Please log in to access this page.'

    @login_manager.user_loader
    def load_user(user_id):
        return User.get_by_id(int(user_id))

    from .routes.auth      import auth_bp
    from .routes.upload    import upload_bp
    from .routes.admin     import admin_bp
    from .routes.manage    import manage_bp
    from .routes.receiving  import receiving_bp
    from .routes.invoicing  import invoicing_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(upload_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(manage_bp)
    app.register_blueprint(receiving_bp)
    app.register_blueprint(invoicing_bp)

    # ── Security (must be after blueprints so CSRF & Talisman cover all routes) ──
    from .security import init_security
    init_security(app)

    return app
