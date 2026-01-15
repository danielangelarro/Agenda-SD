import streamlit as st

def show_login_page(api_client):
    # CSS personalizado para mejorar la apariencia
    st.markdown("""
        <style>
        .login-container {
            max-width: 500px;
            margin: 0 auto;
            padding: 2rem;
        }
        .login-title {
            text-align: center;
            color: #1f77b4;
            font-size: 2.5rem;
            font-weight: bold;
            margin-bottom: 0.5rem;
        }
        .login-subtitle {
            text-align: center;
            color: #666;
            font-size: 1.1rem;
            margin-bottom: 2rem;
        }
        .stTextInput > div > div > input {
            border-radius: 10px;
            border: 2px solid #e0e0e0;
            padding: 0.75rem;
            font-size: 1rem;
        }
        .stTextInput > div > div > input:focus {
            border-color: #1f77b4;
            box-shadow: 0 0 0 0.2rem rgba(31, 119, 180, 0.25);
        }
        .stButton > button {
            width: 100%;
            border-radius: 10px;
            padding: 0.75rem;
            font-size: 1.1rem;
            font-weight: 600;
            transition: all 0.3s ease;
        }
        .stButton > button:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        }
        div[data-testid="stMarkdownContainer"] p {
            text-align: center;
        }
        </style>
    """, unsafe_allow_html=True)

    # Centrar el contenido
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        # Título y subtítulo
        st.markdown('<p class="login-title">📅 Sistema de Agenda</p>', unsafe_allow_html=True)
        
        if "show_register" not in st.session_state:
            st.session_state.show_register = False

        if not st.session_state.show_register:
            # --- LOGIN ---
            st.markdown('<p class="login-subtitle">Inicia sesión para continuar</p>', unsafe_allow_html=True)
            st.markdown("---")
            
            username = st.text_input("👤 Usuario", placeholder="Ingresa tu usuario", key="login_user")
            password = st.text_input("🔒 Contraseña", type="password", placeholder="Ingresa tu contraseña", key="login_pass")

            st.markdown("<br>", unsafe_allow_html=True)
            
            col_btn1, col_btn2 = st.columns(2)
            
            with col_btn1:
                login_btn = st.button("🚀 Iniciar sesión", type="primary", use_container_width=True)
            
            with col_btn2:
                register_btn = st.button("📝 Crear cuenta", use_container_width=True)

            if login_btn:
                # Eliminar espacios en blanco al inicio y final
                username = username.strip()
                password = password.strip()

                # Validaciones en el cliente
                if not username or not password:
                    st.error("❌ Por favor, ingresa usuario y contraseña")
                elif len(username) < 3:
                    st.error("❌ El usuario debe tener al menos 3 caracteres")
                elif len(password) < 3:
                    st.error("❌ La contraseña debe tener al menos 3 caracteres")
                else:
                    try:
                        with st.spinner("Iniciando sesión..."):
                            result = api_client.login(username, password)
                            
                            # Verificar que el resultado tenga los campos esperados
                            if not result or "token" not in result or "user_id" not in result:
                                st.error("❌ Error: Respuesta del servidor inválida")
                                return
                            
                            token = result["token"]
                            user_id = result["user_id"]

                            # Guardar en session state
                            st.session_state.logged_in = True
                            st.session_state.username = username
                            st.session_state.user_id = user_id
                            st.session_state.session_token = token
                            st.session_state.websocket_connected = False
                            st.session_state.notifications = []

                            # Agregar token y user_id a query params para persistencia
                            st.query_params['session_token'] = token
                            st.query_params['user_id'] = str(user_id)

                            st.success("✅ Sesión iniciada correctamente")
                            st.rerun()
                    except Exception as e:
                        st.error(f"❌ {str(e)}")

            if register_btn:
                st.session_state.show_register = True
                st.rerun()

        else:
            # --- REGISTRO ---
            st.markdown('<p class="login-subtitle">Crea tu cuenta nueva</p>', unsafe_allow_html=True)
            st.markdown("---")
            
            username = st.text_input("👤 Usuario", placeholder="Elige un nombre de usuario", key="reg_user")
            password = st.text_input("🔒 Contraseña", type="password", placeholder="Crea una contraseña segura", key="reg_pass")
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            col_btn1, col_btn2 = st.columns(2)
            
            with col_btn1:
                register_btn = st.button("✅ Registrarse", type="primary", use_container_width=True)
            
            with col_btn2:
                back_btn = st.button("◀️ Volver", use_container_width=True)

            if register_btn:
                # Eliminar espacios en blanco al inicio y final
                username = username.strip()
                password = password.strip()

                # Validaciones en el cliente
                if not username or not password:
                    st.error("❌ Usuario y contraseña no pueden estar vacíos")
                elif len(username) < 3:
                    st.error("❌ El usuario debe tener al menos 3 caracteres")
                elif len(password) < 3:
                    st.error("❌ La contraseña debe tener al menos 3 caracteres")
                elif ' ' in username:
                    st.error("❌ El usuario no puede contener espacios")
                else:
                    try:
                        with st.spinner("Creando cuenta..."):
                            result = api_client.register(username, password)
                            st.success("✅ Usuario creado exitosamente. Ahora puedes iniciar sesión")
                            st.session_state.show_register = False
                            st.rerun()
                    except Exception as e:
                        st.error(f"❌ {str(e)}")

            if back_btn:
                st.session_state.show_register = False
                st.rerun()
        
        # Información adicional al pie
        st.markdown("<br><br>", unsafe_allow_html=True)
        st.markdown("---")
        st.markdown(
            '<p style="text-align: center; color: #999; font-size: 0.9rem;">Sistema de Gestión de Agenda Distribuido</p>',
            unsafe_allow_html=True
        )