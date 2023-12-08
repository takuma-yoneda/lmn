#!/usr/bin/env python3
import platform
import torch

print('====================================')

# Obtain the hostname and print it
print('🍋 Running the script on:', platform.node())

# Check if we have access to a GPU
print('🍋 CUDA available:', torch.cuda.is_available())

print('====================================')