
import io
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import streamlit as st

# ================== Streamlit Config ==================
st.set_page_config(page_title="PGD Comparison Tracking", layout="wide")
st.title("📦 PGD Comparison Tracking — SAP vs Infor")
st.caption("Upload 1 SAP Excel file (.xlsx) dan satu atau lebih Infor CSV (.csv). Aplikasi akan merge, cleaning, comparison, visualisasi, dan sediakan unduhan laporan.")

# ================== Utils ==================
@st.cache_data(show_spinner=False)
def read_excel_file(file) -> pd.DataFrame:
    return pd.read_excel(file, engine="openpyxl")

@st.cache_data(show_spinner=False)
def read_csv_file(file) -> pd.DataFrame:
    # best-effort encoding
    for enc in ("utf-8", "utf-8-sig", "latin1"):
        try:
            file.seek(0)
            return pd.read_csv(file, encoding=enc)
        except Exception:
            continue
    file.seek(0)
    return pd.read_csv(file)

def convert_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    date_cols = [
        'Document Date', 'FPD', 'LPD', 'CRD', 'PSDD', 'FCR Date', 'PODD', 'PD', 'PO Date', 'Actual PGI',
        'Infor CRD', 'Infor PD', 'Infor PSDD', 'Infor FPD', 'Infor LPD', 'Infor PODD'
    ]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

def load_sap(sap_df: pd.DataFrame) -> pd.DataFrame:
    df = sap_df.copy()
    if "Quanity" in df.columns and "Quantity" not in df.columns:
        df.rename(columns={'Quanity': 'Quantity'}, inplace=True)
    if "PO No.(Full)" in df.columns:
        df["PO No.(Full)"] = df["PO No.(Full)"].astype(str).str.strip()
    df = convert_date_columns(df)
    return df

def load_infor_from_many_csv(csv_dfs: list[pd.DataFrame]) -> pd.DataFrame:
    data_list = []
    required_cols = [
        'PO Statistical Delivery Date (PSDD)',
        'Customer Request Date (CRD)',
        'Line Aggregator'
    ]
    for i, df in enumerate(csv_dfs, start=1):
        if all(col in df.columns for col in required_cols):
            data_list.append(df)
            st.success(f"Dibaca ✅ CSV ke-{i} (kolom wajib lengkap)")
        else:
            miss = [c for c in required_cols if c not in df.columns]
            st.warning(f"CSV ke-{i} dilewati ⚠️ (kolom wajib hilang: {miss})")

    if not data_list:
        return pd.DataFrame()

    df_all = pd.concat(data_list, ignore_index=True)
    return df_all

def process_infor(df_all: pd.DataFrame) -> pd.DataFrame:
    selected_columns = [
        'Order #', 'Order Status', 'Model Name', 'Article Number', 'Gps Customer Number',
        'Country/Region', 'Customer Request Date (CRD)', 'Plan Date', 'PO Statistical Delivery Date (PSDD)',
        'First Production Date', 'Last Production Date', 'PODD', 'Production Lead Time',
        'Class Code', 'Delay - Confirmation', 'Delay - PO Del Update', 'Quantity'
    ]

    missing_cols = [col for col in selected_columns if col not in df_all.columns]
    if missing_cols:
        st.error(f"Kolom hilang dalam data Infor: {missing_cols}")
        return pd.DataFrame()

    df_infor = df_all[selected_columns].copy()
    df_infor = df_infor.groupby('Order #', as_index=False).agg({
        'Order Status': 'first',
        'Model Name': 'first',
        'Article Number': 'first',
        'Gps Customer Number': 'first',
        'Country/Region': 'first',
        'Customer Request Date (CRD)': 'first',
        'Plan Date': 'first',
        'PO Statistical Delivery Date (PSDD)': 'first',
        'First Production Date': 'first',
        'Last Production Date': 'first',
        'PODD': 'first',
        'Production Lead Time': 'first',
        'Class Code': 'first',
        'Delay - Confirmation': 'first',
        'Delay - PO Del Update': 'first',
        'Quantity': 'sum'
    })

    df_infor["Order #"] = df_infor["Order #"].astype(str).str.zfill(10).str.strip()

    rename_cols = {
        'Order Status': 'Order Status Infor',
        'Model Name': 'Infor Model Name',
        'Article Number': 'Infor Article No',
        'Gps Customer Number': 'Infor GPS Country',
        'Country/Region': 'Infor Ship-to Country',
        'Customer Request Date (CRD)': 'Infor CRD',
        'Plan Date': 'Infor PD',
        'PO Statistical Delivery Date (PSDD)': 'Infor PSDD',
        'First Production Date': 'Infor FPD',
        'Last Production Date': 'Infor LPD',
        'PODD': 'Infor PODD',
        'Production Lead Time': 'Infor Lead time',
        'Class Code': 'Infor Classification Code',
        'Delay - Confirmation': 'Infor Delay/Early - Confirmation CRD',
        'Delay - PO Del Update': 'Infor Delay - PO PSDD Update',
        'Quantity': 'Infor Quantity'
    }
    df_infor.rename(columns=rename_cols, inplace=True)
    st.info(f"Jumlah baris setelah proses Infor: {len(df_infor)}")
    return df_infor

def merge_sap_infor(df_sap: pd.DataFrame, df_infor: pd.DataFrame) -> pd.DataFrame:
    df_sap = df_sap.copy()
    df_infor = df_infor.copy()

    if 'PO No.(Full)' in df_sap.columns:
        df_sap['PO No.(Full)'] = df_sap['PO No.(Full)'].astype(str).str.zfill(10)
    if 'Order #' in df_infor.columns:
        df_infor['Order #'] = df_infor['Order #'].astype(str).str.zfill(10)

    df_merged = df_sap.merge(
        df_infor,
        how='left',
        left_on='PO No.(Full)',
        right_on='Order #'
    )
    return df_merged

def fill_missing_dates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['Order Status Infor'] = df.get('Order Status Infor', pd.Series(dtype=str)).astype(str).str.strip().str.upper()
    for col in ['LPD', 'FPD', 'CRD', 'PD', 'PSDD', 'PODD']:
        if col not in df.columns:
            df[col] = pd.NaT
        df[col] = pd.to_datetime(df[col], errors='coerce')

    mask_open = df['Order Status Infor'].eq('OPEN')
    min_dates = df[['CRD', 'PD']].min(axis=1)

    df.loc[mask_open & df['LPD'].isna(), 'LPD'] = min_dates
    df.loc[mask_open & df['FPD'].isna(), 'FPD'] = min_dates
    df.loc[mask_open & df['PSDD'].isna(), 'PSDD'] = df['CRD']
    df.loc[mask_open & df['PODD'].isna(), 'PODD'] = df['CRD']

    return df

def clean_and_compare(df_merged: pd.DataFrame) -> pd.DataFrame:
    df_merged = df_merged.copy()

    # Step 1: numerik
    numeric_cols = ["Quantity", "Infor Quantity", "Production Lead Time", "Infor Lead time", "Article Lead time"]
    for col in numeric_cols:
        if col in df_merged.columns:
            df_merged[col] = pd.to_numeric(df_merged[col], errors="coerce").fillna(0).round(2)

    # Step 2: mapping delay codes
    code_mapping = {
        '161': '01-0161', '84': '03-0084', '68': '02-0068', '64': '04-0064',
        '62': '02-0062', '61': '01-0061', '51': '03-0051', '46': '03-0046',
        '7': '02-0007', '3': '03-0003', '2': '01-0002', '1': '01-0001',
        '4': '04-0004', '8': '02-0008', '10': '04-0010', '49': '03-0049',
        '90': '04-0090', '63' : '03-0063'
    }
    def map_code_safely(x):
        try:
            return code_mapping.get(str(int(float(x))), x)
        except (ValueError, TypeError):
            return x

    if "Infor Delay/Early - Confirmation CRD" in df_merged.columns:
        df_merged["Infor Delay/Early - Confirmation CRD"] = (
            df_merged["Infor Delay/Early - Confirmation CRD"]
            .replace(['--', 'N/A', 'NULL'], pd.NA)
            .apply(map_code_safely)
        )
    if "Infor Delay - PO PSDD Update" in df_merged.columns:
        df_merged["Infor Delay - PO PSDD Update"] = (
            df_merged["Infor Delay - PO PSDD Update"]
            .replace(['--', 'N/A', 'NULL'], pd.NA)
            .apply(map_code_safely)
        )

    # Step 3: normalisasi string
    string_cols = [
        "Model Name", "Infor Model Name", "Article No", "Infor Article No",
        "Classification Code", "Infor Classification Code",
        "Ship-to Country", "Infor Ship-to Country",
        "Ship-to-Sort1", "Infor GPS Country",
        "Delay/Early - Confirmation CRD", "Infor Delay/Early - Confirmation CRD",
        "Delay - PO PSDD Update", "Infor Delay - PO PSDD Update"
    ]
    for col in string_cols:
        if col in df_merged.columns:
            df_merged[col] = df_merged[col].astype(str).str.strip().str.upper()

    if "Ship-to-Sort1" in df_merged.columns:
        df_merged["Ship-to-Sort1"] = df_merged["Ship-to-Sort1"].astype(str).str.strip().str.replace(".0", "", regex=False)
    if "Infor GPS Country" in df_merged.columns:
        df_merged["Infor GPS Country"] = df_merged["Infor GPS Country"].astype(str).str.strip().str.replace(".0", "", regex=False)

    if "Infor Delay/Early - Confirmation CRD" in df_merged.columns:
        df_merged["Infor Delay/Early - Confirmation CRD"] = df_merged["Infor Delay/Early - Confirmation CRD"].replace(['--', 'N/A', 'NULL'], pd.NA)

    # Step 4: hasil perbandingan
    def safe_result(col1, col2):
        if col1 in df_merged.columns and col2 in df_merged.columns:
            return np.where(df_merged[col1] == df_merged[col2], "TRUE", "FALSE")
        else:
            return ["COLUMN MISSING"] * len(df_merged)

    df_merged["Result_Quantity"] = safe_result("Quantity", "Infor Quantity")
    df_merged["Result_Model Name"] = safe_result("Model Name", "Infor Model Name")
    df_merged["Result_Article No"] = safe_result("Article No", "Infor Article No")
    df_merged["Result_Classification Code"] = safe_result("Classification Code", "Infor Classification Code")
    df_merged["Result_Delay_CRD"] = safe_result("Delay/Early - Confirmation CRD", "Infor Delay/Early - Confirmation CRD")
    df_merged["Result_Delay_PSDD"] = safe_result("Delay - PO PSDD Update", "Infor Delay - PO PSDD Update")
    df_merged["Result_Lead Time"] = safe_result("Article Lead time", "Infor Lead time")
    df_merged["Result_Country"] = safe_result("Ship-to Country", "Infor Ship-to Country")
    df_merged["Result_Sort1"] = safe_result("Ship-to-Sort1", "Infor GPS Country")
    df_merged["Result_FPD"] = safe_result("FPD", "Infor FPD")
    df_merged["Result_LPD"] = safe_result("LPD", "Infor LPD")
    df_merged["Result_CRD"] = safe_result("CRD", "Infor CRD")
    df_merged["Result_PSDD"] = safe_result("PSDD", "Infor PSDD")
    df_merged["Result_PODD"] = safe_result("PODD", "Infor PODD")
    df_merged["Result_PD"] = safe_result("PD", "Infor PD")

    return df_merged

DESIRED_ORDER = [
    'Client No', 'Site', 'Brand FTY Name', 'SO', 'Order Type', 'Order Type Description',
    'PO No.(Full)', 'Order Status Infor', 'PO No.(Short)', 'Merchandise Category 2', 'Quantity',
    'Infor Quantity', 'Result_Quantity', 'Model Name', 'Infor Model Name', 'Result_Model Name',
    'Article No', 'Infor Article No', 'Result_Article No', 'SAP Material', 'Pattern Code(Up.No.)',
    'Model No', 'Outsole Mold', 'Gender', 'Category 1', 'Category 2', 'Category 3', 'Unit Price',
    'Classification Code', 'Infor Classification Code', 'Result_Classification Code', 'DRC',
    'Delay/Early - Confirmation PD', 'Delay/Early - Confirmation CRD', 'Infor Delay/Early - Confirmation CRD',
    'Result_Delay_CRD', 'Delay - PO PSDD Update', 'Infor Delay - PO PSDD Update', 'Result_Delay_PSDD',
    'Delay - PO PD Update', 'MDP', 'PDP', 'SDP', 'Article Lead time', 'Infor Lead time',
    'Result_Lead Time', 'Cust Ord No', 'Ship-to-Sort1', 'Infor GPS Country', 'Result_Sort1',
    'Ship-to Country', 'Infor Ship-to Country', 'Result_Country',
    'Ship to Name', 'Document Date', 'FPD', 'Infor FPD', 'Result_FPD', 'LPD', 'Infor LPD',
    'Result_LPD', 'CRD', 'Infor CRD', 'Result_CRD', 'PSDD', 'Infor PSDD', 'Result_PSDD',
    'FCR Date', 'PODD', 'Infor PODD', 'Result_PODD', 'PD', 'Infor PD', 'Result_PD',
    'PO Date', 'Actual PGI', 'Segment', 'S&P LPD', 'Currency'
]

def reorder_columns(df: pd.DataFrame, desired_order: list[str]) -> pd.DataFrame:
    existing_cols = [col for col in desired_order if col in df.columns]
    tail_cols = [c for c in df.columns if c not in existing_cols]
    return df[existing_cols + tail_cols]

def build_report(df_sap: pd.DataFrame, df_infor_raw: pd.DataFrame) -> pd.DataFrame:
    df_infor = process_infor(df_infor_raw)
    if df_infor.empty:
        return pd.DataFrame()

    df_sap2 = convert_date_columns(load_sap(df_sap))
    df_infor2 = convert_date_columns(df_infor)

    df_merged = merge_sap_infor(df_sap2, df_infor2)
    df_merged = fill_missing_dates(df_merged)
    df_final = clean_and_compare(df_merged)
    df_final = reorder_columns(df_final, DESIRED_ORDER)
    return df_final

# ================== Sidebar Uploads ==================
with st.sidebar:
    st.header("📤 Upload Files")
    sap_file = st.file_uploader("SAP Excel (.xlsx)", type=["xlsx"])
    infor_files = st.file_uploader("Infor CSV (boleh multi-file)", type=["csv"], accept_multiple_files=True)
    st.markdown("""
**Tips:**
- Pastikan kolom kunci tersedia:
  - SAP: `PO No.(Full)`, `Quantity`, dan kolom tanggal terkait bila ada.
  - Infor CSV: `PO Statistical Delivery Date (PSDD)`, `Customer Request Date (CRD)`, `Line Aggregator` minimal, sisanya sesuai daftar app.
""")

# ================== Main ==================
if sap_file and infor_files:
    with st.status("Membaca & menggabungkan file...", expanded=True) as status:
        sap_df = read_excel_file(sap_file)
        st.write("SAP dibaca:", sap_df.shape)

        infor_csv_dfs = [read_csv_file(f) for f in infor_files]
        infor_all = load_infor_from_many_csv(infor_csv_dfs)
        st.write("Total Infor (gabungan CSV):", infor_all.shape)

        if infor_all.empty:
            status.update(label="Gagal: tidak ada CSV Infor yang valid.", state="error")
        else:
            status.update(label="Sukses membaca semua file. Lanjut proses...", state="running")
            final_df = build_report(sap_df, infor_all)

            if final_df.empty:
                status.update(label="Gagal membuat report — periksa kolom wajib.", state="error")
            else:
                status.update(label="Report siap! ✅", state="complete")

                # ===== Preview =====
                st.subheader("🔎 Preview Hasil")
                st.dataframe(final_df.head(100), use_container_width=True)

                # ===== Sample merge check =====
                merge_cols = [c for c in ["PO No.(Full)", "Quantity", "Infor Quantity"] if c in final_df.columns]
                if merge_cols:
                    st.markdown("**Sample cek hasil merge (PO & Quantity)**")
                    st.dataframe(final_df[merge_cols].head(10), use_container_width=True)

                # ===== Visualization: TRUE/FALSE counts =====
                st.subheader("📊 Comparison Summary (TRUE vs FALSE)")
                result_cols = [
                    "Result_Quantity", "Result_FPD", "Result_LPD",
                    "Result_CRD", "Result_PSDD", "Result_PODD", "Result_PD"
                ]
                existing_results = [c for c in result_cols if c in final_df.columns]
                if existing_results:
                    true_counts = [int(final_df[c].eq("TRUE").sum()) for c in existing_results]
                    false_counts = [int(final_df[c].eq("FALSE").sum()) for c in existing_results]
                    total_counts = [int(final_df[c].isin(["TRUE","FALSE"]).sum()) for c in existing_results]
                    accuracy = [(t / tot * 100.0) if tot > 0 else 0.0 for t, tot in zip(true_counts, total_counts)]

                    summary_df = pd.DataFrame({
                        "Metric": existing_results,
                        "TRUE": true_counts,
                        "FALSE": false_counts,
                        "Total (TRUE+FALSE)": total_counts,
                        "TRUE %": [round(a, 2) for a in accuracy],
                    })

                    st.dataframe(summary_df, use_container_width=True)

                    # Bar chart
                    chart_df = summary_df.set_index("Metric")[["TRUE", "FALSE"]]
                    st.bar_chart(chart_df)

                    # Pie chart for FALSE distribution
                    st.markdown("**Distribusi FALSE per metric (chart lingkaran)**")
                    try:
                        import matplotlib.pyplot as plt
                        fig, ax = plt.subplots()
                        if sum(false_counts) > 0:
                            ax.pie(
                                false_counts,
                                labels=existing_results,
                                autopct=lambda p: f"{int(round(p*sum(false_counts)/100.0))} ({p:.1f}%)",
                                startangle=90
                            )
                            ax.axis("equal")
                            st.pyplot(fig, use_container_width=True)
                        else:
                            st.info("Tidak ada nilai FALSE pada metric yang dipilih.")
                    except Exception as e:
                        st.warning(f"Gagal menampilkan pie chart: {e}")

                    # FALSE counts table only
                    st.markdown("**Ringkasan jumlah FALSE per metric**")
                    false_df = pd.DataFrame({
                        "Metric": existing_results,
                        "FALSE": false_counts
                    })
                    st.dataframe(false_df, use_container_width=True)
                else:
                    st.info("Kolom hasil perbandingan (Result_*) belum tersedia di data final.")

                # ===== Downloads =====
                today_str = datetime.now(ZoneInfo("Asia/Jakarta")).strftime("%Y%m%d")
                out_name_xlsx = f"PGD Comparison Tracking Report - {today_str}.xlsx"
                out_name_csv = f"PGD Comparison Tracking Report - {today_str}.csv"

                # Excel buffer
                towrite = io.BytesIO()
                with pd.ExcelWriter(towrite, engine="openpyxl") as writer:
                    final_df.to_excel(writer, index=False, sheet_name="Report")
                towrite.seek(0)

                st.download_button(
                    label="⬇️ Download Excel",
                    data=towrite,
                    file_name=out_name_xlsx,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )

                st.download_button(
                    label="⬇️ Download CSV",
                    data=final_df.to_csv(index=False).encode("utf-8"),
                    file_name=out_name_csv,
                    mime="text/csv",
                    use_container_width=True
                )
else:
    st.info("Unggah 1 file SAP (.xlsx) dan satu atau lebih file Infor (.csv) di sidebar untuk mulai memproses.")
