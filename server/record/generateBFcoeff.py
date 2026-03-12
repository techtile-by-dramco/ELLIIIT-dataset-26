import numpy as np
import cvxpy as cp
import re


# %%
def CSIgenerator2(filename):
    """
    Reads CSI data from a text file and returns:
    - CSI_matrix: NumPy array of complex CSI values (1 per unique AP)
    - unique_APs: List of unique AP names (sorted by first appearance)
    """

    with open(filename, "r") as file:
        lines = file.readlines()

    APs = []
    CSI = []

    pattern = re.compile(r"(\w+):\s*Phi_CSI=([-\d\.]+),\s*avg_ampl=([\d\.]+)")

    for line in lines:
        match = pattern.search(line)
        if match:
            ap_name = match.group(1)
            phi = float(match.group(2))
            ampl = float(match.group(3))

            APs.append(ap_name)
            CSI.append(ampl * np.exp(1j * phi))

    # Map unique APs to row indices
    unique_APs, idx_map = np.unique(APs, return_inverse=True)

    # Initialize CSI matrix
    CSI_matrix = np.zeros(len(unique_APs), dtype=complex)
    for i, val in enumerate(CSI):
        CSI_matrix[idx_map[i]] = val

    return CSI_matrix.reshape(-1, 1), list(unique_APs)


# %%
def dominant_eigenvector(X):
    # Compute the dominant eigenvector (eigenvector of the largest eigenvalue)
    eigvals, eigvecs = np.linalg.eigh(X)
    idx = np.argmax(eigvals)
    return eigvecs[:, idx]


# %%
def sdr_solver(H_DL, H_BD, M, scale, alpha, P_max):
    # For the given channel coefficients, solve the proposed problem and provide proposed BF vector
    # Compute M_BD and M_DL
    M_BD = H_BD.conj().T @ H_BD
    M_DL = H_DL.conj().T @ H_DL

    # Define the semidefinite variable (Hermitian)
    X_new = cp.Variable((M, M), hermitian=True)

    # Objective: maximize scale * trace(M_BD * X_new)
    objective = cp.Maximize(scale * cp.real(cp.trace(M_BD @ X_new)))

    # Constraints
    constraints = [
        cp.real(cp.trace(scale * (M_DL - alpha * M_BD) @ X_new)) <= 0,
        X_new >> 0,  # Hermitian positive semidefinite constraint
    ]

    # Add per-antenna power constraints
    for i in range(M):
        constraints.append(cp.real(X_new[i, i]) <= P_max)

    # Problem definition and solve
    prob = cp.Problem(objective, constraints)
    prob.solve(
        solver=cp.SCS, verbose=False
    )  # You can try other solvers, e.g., 'CVXOPT' or 'MOSEK' or 'SCS'

    if X_new.value is None:
        raise ValueError("Optimization did not converge.")

    # Extract dominant eigenvector
    w_optimum = dominant_eigenvector(X_new.value)

    # Normalize the beamforming vector
    w = w_optimum / np.max(np.abs(w_optimum))

    return w


# %%
def cvx_solver(H_DL, h_C, M, scale, alpha, P_max):
    # For the given channel coefficients, solve the proposed problem and provide proposed BF vector

    # Define the semidefinite variable (Hermitian)
    x = cp.Variable(M, complex=True)

    # Objective: maximize scale * trace(M_BD * X_new)
    objective = cp.Maximize(scale * cp.real(h_C.T @ x))

    # Constraints
    constraints = []

    # Null constraint: scale * H_DL_prime * x == 0
    constraints.append(scale * H_DL @ x == 0)

    # Add per-antenna power constraints
    for i in range(M):
        constraints.append(cp.abs(x[i]) <= np.sqrt(P_max))

    # Problem definition and solve
    prob = cp.Problem(objective, constraints)
    prob.solve(
        solver=cp.MOSEK, verbose=False
    )  # You can try other solvers, e.g., 'CVXOPT' or 'MOSEK' or 'SCS'

    if x.value is None:
        raise ValueError("Optimization did not converge.")

    # Solution: normalize beamforming vector
    w = x.value / np.max(np.abs(x.value))

    return w


def compute_bf_phases(
    bf_type: str,
    alpha: float,
    scale: float,
    f_c: float,
    file_bd: str,
    file_reader: str,
    write_output: bool = False,
):
    # Computes beamforming (BF) phases for a given channel setup.

    # Parameters:
    # -----------
    # bf_type : str
    #     Type of beamforming to use. Options:
    #         - 'cvx': Optimal beamforming using a convex optimization solver.
    #         - 'mrt': Maximum Ratio Transmission beamforming.

    # alpha : float
    #     Weighting parameter for the optimization (used in CVX solver).

    # scale : float
    #     Scaling factor used in the optimization objective (for CVX).

    # f_c : float
    #     Carrier frequency in Hz, used to compute wavelength.

    # file_bd : str
    #     Path to the file containing the backscatter device (BD) channel data.

    # file_reader : str
    #     Path to the file containing the reader (downlink) channel data.

    # write_output : bool, optional
    #     If True, writes the resulting beamforming phases to a text file.
    #     Default is False.

    # Returns:
    # --------
    # w_angle : np.ndarray
    #     Beamforming phases (in radians) for each AP (Access Point).

    # unique_APs : list
    #     List of identifiers for each AP, corresponding to the beamforming phases.
    # Read the channel data
    h_C, unique_APs = CSIgenerator2(file_bd)
    H_DL, _ = CSIgenerator2(file_reader)

    # Ensure H_DL is a row vector
    if H_DL.shape[0] != 1:
        H_DL = H_DL.T

    # Constants
    c = 3e8  # Speed of light in m/s
    _lambda = c / f_c  # Wavelength

    # Distance and channel
    distance = 1
    h_R = _lambda / (4 * np.pi * distance)
    h_R = np.array([h_R])[:, np.newaxis]

    # Channels
    H_BD = h_R * h_C.T

    # Dimensions
    M = len(h_C)
    N = len(h_R)
    P_max = 1

    # Beamforming
    if bf_type.lower() == "cvx":
        if alpha == 0:
            w = cvx_solver(H_DL, h_C, M, scale, alpha, P_max)
        else:
            w = sdr_solver(H_DL, H_BD, M, scale, alpha, P_max)
    elif bf_type.lower() == "mrt":
        w = np.conj(h_C) / np.abs(h_C)
        w = w / np.linalg.norm(w)
    else:
        raise ValueError("Unsupported BF type. Use 'cvx' or 'mrt'.")

    # Extract phase
    w_angle = np.angle(w)

    # Compute constraint and objective values
    const = (np.linalg.norm(H_DL @ w) / np.linalg.norm(H_BD @ w)) ** 2
    obj = (np.linalg.norm(H_BD @ w)) ** 2

    print(f"Constraint is {const:.9f}")
    print(f"Objective is {obj:.9f}\n")

    # Write to file if requested
    if write_output:
        filename = "Proposed_BF.txt" if bf_type.lower() == "cvx" else "MRT_BF.txt"
        with open(filename, "w") as f:
            for name, angle in zip(unique_APs, w_angle):
                f.write(f"{name}: {angle.item():.8f}\n")

    return w_angle, unique_APs


phases, AP_list = compute_bf_phases(
    bf_type="cvx",
    alpha=0,
    scale=1e1,
    f_c=0.92e9,
    file_bd="Processed_Result_BD.txt",
    file_reader="Processed_Result_Reader.txt",
    write_output=True,
)
